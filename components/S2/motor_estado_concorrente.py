
"""Motor do componente S2 — Regras temporais e eventos (arquitetura 6c).

Consome mensagens ods.i2.tracks, ods.s1.position, ods.s1.zone_transition,
ods.s1.zone_occupancy e, quando instalado, ods.i9.ppe_compliance. Publica
mensagens nos contratos ods.s2.entity e ods.s2.event, usando o envelope comum
(schema_version 1.0). Não recalcula geometria: só compara position_m quando as
observações compartilham space_id; coordenadas point_px não fundem câmeras.
"""
from __future__ import annotations

import math
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

try:  # execução como pacote: python -m components.S2.main
    from .config import (
        ARTIFACT_VERSION_FUSAO, DISTANCIA_FUSAO_M, SCHEMA_VERSION,
        SINCRONISMO_FUSAO_MS, TTL_ENTIDADE_MS, REIDENTIFICACAO_PIXELS,
    )
    from .filtro_ruido_temporal import FiltroRuidoTemporal
except ImportError:  # execução direta a partir de components/S2
    from config import (
        ARTIFACT_VERSION_FUSAO, DISTANCIA_FUSAO_M, SCHEMA_VERSION,
        SINCRONISMO_FUSAO_MS, TTL_ENTIDADE_MS, REIDENTIFICACAO_PIXELS,
    )
    from filtro_ruido_temporal import FiltroRuidoTemporal


SCHEMAS_CONSUMIDOS = {
    "ods.i2.tracks",
    "ods.i9.ppe_compliance",
    "ods.s1.position",
    "ods.s1.zone_transition",
    "ods.s1.zone_occupancy",
    "ods.session.started",
    "ods.s2.rules.command",
}
SCHEMAS_POR_QUADRO = {
    "ods.i2.tracks",
    "ods.s1.position",
    "ods.i9.ppe_compliance",
}
EVENT_TYPES = {
    "ZONE_ENTER", "ZONE_EXIT", "DWELL_EXCEEDED", "OCCUPANCY_ABOVE",
    "OCCUPANCY_BELOW", "PPE_NON_COMPLIANT", "OBJECT_STATIONARY",
    "CROSS_CAMERA_MATCH",
}


def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _iso_de_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _uuid_valido(valor: Any, campo: str) -> str:
    if not isinstance(valor, str):
        raise ValueError(f"Campo obrigatório {campo!r} deve ser um UUID em string")
    try:
        return str(uuid.UUID(valor))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Campo obrigatório {campo!r} não contém UUID válido: {valor!r}") from exc


def _payload(mensagem: dict[str, Any]) -> dict[str, Any]:
    valor = mensagem.get("payload")
    if not isinstance(valor, dict):
        raise ValueError(f"A mensagem {mensagem.get('schema')!r} precisa ter payload do tipo object")
    return valor


class MotorGerenciamentoEstadoConcorrente:
    """Implementa fusão de identidade, estado local e qualificação temporal de eventos."""

    def __init__(
        self,
        merge_distance_threshold: float = DISTANCIA_FUSAO_M,
        time_sync_threshold_ms: int = SINCRONISMO_FUSAO_MS,
        filtro_temporal: FiltroRuidoTemporal | None = None,
        ttl_threshold_ms: int = TTL_ENTIDADE_MS,
    ) -> None:
        if merge_distance_threshold < 0 or time_sync_threshold_ms < 0 or ttl_threshold_ms < 0:
            raise ValueError("Os limiares de fusão e TTL devem ser maiores ou iguais a zero")
        self.merge_distance_threshold = float(merge_distance_threshold)
        self.time_sync_threshold_ms = int(time_sync_threshold_ms)
        self.ttl_threshold_ms = int(ttl_threshold_ms)
        self.filtro_temporal = filtro_temporal or FiltroRuidoTemporal()
        self.estado_em_memoria: dict[str, dict[str, Any]] = {}
        self.global_id_counter = 1
        self.artifact_version_regras: str | None = None
        self.regras: list[dict[str, Any]] = []
        self._track_to_entity: dict[tuple[str, str, str], str] = {}
        self._ppe_por_rastro: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
        self._condicoes: dict[tuple[Any, ...], dict[str, Any]] = {}
        self._contagem_zonas: dict[tuple[str, str], int] = {}
        self._inicio_zona: dict[tuple[str, str], int] = {}
        self._posicao_base: dict[tuple[str, str], tuple[int, float, float, str]] = {}
        self._sequence_por_sessao: dict[str, int] = defaultdict(int)
        self._processadas: set[str] = set()
        self._ultima_observacao_entidade: dict[tuple[str, str], int] = {}
        self._ultima_observacao_rastro: dict[tuple[str, str, str], int] = {}
        self._estado_entidades: dict[str, dict[str, Any]] = {}

    def carregar_regras(self, artefato_regras: dict[str, Any] | None) -> None:
        """Carrega o payload do artefato RULES (mesmo formato do rules.command)."""
        if artefato_regras is None:
            self.artifact_version_regras = None
            self.regras = []
            return
        if not isinstance(artefato_regras, dict):
            raise ValueError("O artefato RULES deve ser um objeto JSON")
        versao = artefato_regras.get("artifact_version")
        regras = artefato_regras.get("rules")
        if not isinstance(versao, str) or not versao.strip():
            raise ValueError("O artefato RULES exige artifact_version não vazio")
        if not isinstance(regras, list):
            raise ValueError("O artefato RULES exige rules como array")
        validadas: list[dict[str, Any]] = []
        ids: set[str] = set()
        for indice, regra_original in enumerate(regras):
            if not isinstance(regra_original, dict):
                raise ValueError(f"rules[{indice}] deve ser um object")
            regra = dict(regra_original)
            obrigatorios = ("rule_id", "event_type", "min_confidence", "uncertain_below", "min_duration_ms", "debounce_ms")
            faltantes = [campo for campo in obrigatorios if campo not in regra]
            if faltantes:
                raise ValueError(f"rules[{indice}] sem campos obrigatórios: {', '.join(faltantes)}")
            if not isinstance(regra["rule_id"], str) or not regra["rule_id"].strip():
                raise ValueError(f"rules[{indice}].rule_id deve ser string não vazia")
            if regra["rule_id"] in ids:
                raise ValueError(f"rule_id duplicado: {regra['rule_id']}")
            ids.add(regra["rule_id"])
            if regra["event_type"] not in EVENT_TYPES:
                raise ValueError(f"UNKNOWN_EVENT_TYPE: {regra['event_type']!r}")
            min_conf = regra["min_confidence"]
            incerta = regra["uncertain_below"]
            if not isinstance(min_conf, (int, float)) or isinstance(min_conf, bool) or not 0 <= min_conf <= 1:
                raise ValueError(f"rules[{indice}].min_confidence deve estar entre 0 e 1")
            if not isinstance(incerta, (int, float)) or isinstance(incerta, bool) or not min_conf <= incerta <= 1:
                raise ValueError(f"rules[{indice}].uncertain_below deve estar entre min_confidence e 1")
            for campo in ("min_duration_ms", "debounce_ms"):
                valor = regra[campo]
                if not isinstance(valor, int) or isinstance(valor, bool) or valor < 0:
                    raise ValueError(f"rules[{indice}].{campo} deve ser integer >= 0")
            if "threshold" in regra and (not isinstance(regra["threshold"], (int, float)) or isinstance(regra["threshold"], bool)):
                raise ValueError(f"rules[{indice}].threshold deve ser number")
            validadas.append(regra)
        self.artifact_version_regras = versao
        self.regras = validadas

    def atualizar_estado_memoria(self, evento: dict[str, Any]) -> None:
        """Registra a última observação de um rastro local sem substituir IDs globais."""
        session_id = evento.get("session_id", "")
        source_id = evento.get("source_id", "")
        track_id = evento.get("track_id", "")
        if track_id:
            chave = f"{session_id}:{source_id}:{track_id}"
            self.estado_em_memoria[chave] = dict(evento)

    def _novo_entity_id(self) -> str:
        entity_id = f"e-{self.global_id_counter:06d}"
        self.global_id_counter += 1
        return entity_id

    def _entity_para_rastro(self, session_id: str, source_id: str, track_id: str) -> str:
        chave = (session_id, source_id, track_id)
        if chave not in self._track_to_entity:
            self._track_to_entity[chave] = self._novo_entity_id()
        return self._track_to_entity[chave]

    def _proxima_sequence(self, session_id: str) -> int:
        sequence = self._sequence_por_sessao[session_id]
        self._sequence_por_sessao[session_id] += 1
        return sequence

    def _envelope_evento(
        self,
        schema: str,
        session_id: str,
        captured_at_ms: int,
        source_id: str | None,
        artifact_version: str | None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        mensagem: dict[str, Any] = {
            "message_type": "event",
            "message_id": str(uuid.uuid4()),
            "schema": schema,
            "schema_version": SCHEMA_VERSION,
            "producer": "S2",
            "published_at": _agora_iso(),
            "captured_at": _iso_de_ms(captured_at_ms),
            "session_id": session_id,
            "sequence": self._proxima_sequence(session_id),
            "payload": {},
        }
        if source_id:
            mensagem["source_id"] = source_id
        if artifact_version:
            mensagem["artifact_version"] = artifact_version
        if correlation_id:
            mensagem["correlation_id"] = correlation_id
        return mensagem

    @staticmethod
    def _validar_mensagem(mensagem: Any, indice: int) -> dict[str, Any]:
        if not isinstance(mensagem, dict):
            raise ValueError(f"Mensagem na posição {indice} não é um objeto JSON")
        obrigatorios = ("message_type", "message_id", "schema", "schema_version", "producer", "published_at", "session_id", "sequence", "payload")
        faltantes = [campo for campo in obrigatorios if campo not in mensagem]
        if faltantes:
            raise ValueError(
                f"Mensagem na posição {indice} não segue o envelope comum 1.0; faltam: {', '.join(faltantes)}. "
                "Regenere a entrada no formato da arquitetura 6c."
            )
        if mensagem["message_type"] != "event":
            raise ValueError(f"S2 esperava message_type='event', recebeu {mensagem['message_type']!r}")
        _uuid_valido(mensagem["message_id"], "message_id")
        _uuid_valido(mensagem["session_id"], "session_id")
        if not isinstance(mensagem["schema"], str) or not isinstance(mensagem["schema_version"], str):
            raise ValueError("schema e schema_version devem ser strings")
        if mensagem["schema_version"].split(".")[0] != "1":
            raise ValueError(f"Versão de schema incompatível: {mensagem['schema_version']!r}")
        if not isinstance(mensagem["sequence"], int) or isinstance(mensagem["sequence"], bool) or mensagem["sequence"] < 0:
            raise ValueError("sequence deve ser integer >= 0")
        FiltroRuidoTemporal.timestamp_em_ms(mensagem["published_at"])
        if "captured_at" in mensagem:
            FiltroRuidoTemporal.timestamp_em_ms(mensagem["captured_at"])
        elif mensagem["schema"] not in {"ods.session.started"}:
            raise ValueError(f"A mensagem {mensagem['schema']!r} precisa ter captured_at para o perfil S2")
        _payload(mensagem)
        return mensagem

    def fusao_e_deduplicacao(self, eventos_validos: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Processa mensagens de entrada e retorna ods.s2.entity + ods.s2.event.

        A entrada deve conter objetos JSON com envelope comum, não eventos crus.
        Duplicatas são descartadas por message_id, conforme a arquitetura.
        """
        mensagens: list[dict[str, Any]] = []
        comandos_regras: list[dict[str, Any]] = []
        for i, bruto in enumerate(eventos_validos):
            if isinstance(bruto, dict) and bruto.get("schema") == "ods.s2.rules.command":
                comando = self._validar_comando_regras(bruto, i)
                if comando["message_id"] in self._processadas:
                    continue
                self._processadas.add(comando["message_id"])
                comandos_regras.append(comando)
                continue

            mensagem = self._validar_mensagem(bruto, i)
            schema = mensagem["schema"]
            if schema not in SCHEMAS_CONSUMIDOS:
                raise ValueError(f"S2 não consome o schema {schema!r}; confira o mapa de produção/consumo da arquitetura 6c")
            message_id = mensagem["message_id"]
            if message_id in self._processadas:
                continue
            self._processadas.add(message_id)
            mensagens.append(mensagem)

        quadros: dict[tuple[str, str, int], dict[str, Any]] = {}
        auxiliares: list[dict[str, Any]] = []
        for mensagem in mensagens:
            schema = mensagem["schema"]
            if schema in SCHEMAS_POR_QUADRO:
                payload = _payload(mensagem)
                if "frame_index" not in payload or not isinstance(payload["frame_index"], int):
                    raise ValueError(f"{schema} exige payload.frame_index integer")
                chave = (mensagem["session_id"], mensagem.get("source_id", ""), payload["frame_index"])
                grupo = quadros.setdefault(chave, {"mensagens": {}, "session_id": chave[0], "source_id": chave[1], "frame_index": chave[2]})
                if schema == "ods.i9.ppe_compliance":
                    grupo["mensagens"].setdefault(schema, []).append(mensagem)
                else:
                    if schema in grupo["mensagens"]:
                        raise ValueError(
                            f"Mais de uma mensagem {schema} para source_id/frame_index {chave[1:]}; "
                            "o contrato exige uma mensagem por quadro"
                        )
                    grupo["mensagens"][schema] = mensagem
            else:
                auxiliares.append(mensagem)

        batches = self._agrupar_instantes_de_fusao(list(quadros.values()))
        timeline: list[tuple[int, int, Any]] = []
        for batch in batches:
            timestamp = min(
                self._captura_ms(mensagem)
                for quadro in batch
                for valor in quadro["mensagens"].values()
                for mensagem in (valor if isinstance(valor, list) else [valor])
            )
            timeline.append((timestamp, 0, batch))
        for mensagem in auxiliares:
            ts = self._captura_ms(mensagem) if "captured_at" in mensagem else FiltroRuidoTemporal.timestamp_em_ms(mensagem["published_at"])
            timeline.append((ts, 1, mensagem))
        for comando in comandos_regras:
            ts = FiltroRuidoTemporal.timestamp_em_ms(comando["published_at"])
            timeline.append((ts, -1, comando))
        timeline.sort(key=lambda item: (item[0], item[1]))

        saida: list[dict[str, Any]] = []
        for timestamp_ms, tipo, item in timeline:
            if tipo == -1:
                saida.append(self._processar_comando_regras(item))
            elif tipo == 0:
                mensagem_entidade, entidades, observacoes = self._processar_batch(item, timestamp_ms)
                saida.append(mensagem_entidade)
                saida.extend(self._avaliar_regras_por_entidade(entidades, observacoes, item, timestamp_ms))
                self._encerrar_condicoes_de_entidades_ausentes(entidades, timestamp_ms, item[0]["session_id"], saida)
            else:
                schema = item["schema"]
                if schema == "ods.session.started":
                    self._resetar_estado_sessao(item["session_id"])
                elif schema == "ods.s1.zone_transition":
                    saida.extend(self._processar_transicao_zona(item, timestamp_ms))
                elif schema == "ods.s1.zone_occupancy":
                    saida.extend(self._processar_ocupacao(item, timestamp_ms))
        return saida

    @staticmethod
    def _validar_comando_regras(mensagem: Any, indice: int) -> dict[str, Any]:
        """Valida o perfil CMD de ods.s2.rules.command (sem campos de sinal)."""
        if not isinstance(mensagem, dict):
            raise ValueError(f"Comando na posicao {indice} nao e um objeto JSON")
        obrigatorios = ("message_type", "message_id", "schema", "schema_version", "producer", "published_at", "target", "payload")
        faltantes = [campo for campo in obrigatorios if campo not in mensagem]
        if faltantes:
            raise ValueError(f"ods.s2.rules.command sem campos obrigatorios: {', '.join(faltantes)}")
        if mensagem["message_type"] != "command" or mensagem["schema"] != "ods.s2.rules.command":
            raise ValueError("Comando de regras precisa usar message_type='command' e schema='ods.s2.rules.command'")
        if mensagem["target"] != "S2":
            raise ValueError("ods.s2.rules.command exige target='S2'")
        _uuid_valido(mensagem["message_id"], "message_id")
        if not isinstance(mensagem["schema_version"], str) or mensagem["schema_version"].split(".")[0] != "1":
            raise ValueError("Versao de schema incompativel em ods.s2.rules.command")
        FiltroRuidoTemporal.timestamp_em_ms(mensagem["published_at"])
        if not isinstance(mensagem["payload"], dict):
            raise ValueError("ods.s2.rules.command exige payload do tipo object")
        if not isinstance(mensagem["payload"].get("artifact_version"), str) or not isinstance(mensagem["payload"].get("rules"), list):
            raise ValueError("payload de ods.s2.rules.command exige artifact_version string e rules array")
        return mensagem

    def _processar_comando_regras(self, comando: dict[str, Any]) -> dict[str, Any]:
        """Aplica o artefato RULES solicitado e devolve a reply contratual."""
        payload = comando["payload"]
        try:
            self.carregar_regras({
                "artifact_version": payload["artifact_version"],
                "rules": payload["rules"],
            })
            resposta = {"status": "OK", "applied_rule_ids": [r["rule_id"] for r in self.regras]}
        except ValueError as exc:
            texto = str(exc)
            codigo = "UNKNOWN_EVENT_TYPE" if "UNKNOWN_EVENT_TYPE" in texto else "INVALID_RULE"
            resposta = {"status": "ERROR", "code": codigo, "message": texto}
        return {
            "message_type": "reply",
            "message_id": str(uuid.uuid4()),
            "schema": "ods.s2.rules.reply",
            "schema_version": SCHEMA_VERSION,
            "producer": "S2",
            "published_at": _agora_iso(),
            "correlation_id": comando["message_id"],
            "payload": resposta,
        }

    def processar_mensagens(self, mensagens: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Alias de nomenclatura para chamadas novas."""
        return self.fusao_e_deduplicacao(mensagens)

    @staticmethod
    def _captura_ms(mensagem: dict[str, Any]) -> int:
        valor = mensagem.get("captured_at", mensagem.get("published_at"))
        return FiltroRuidoTemporal.timestamp_em_ms(valor)

    def _agrupar_instantes_de_fusao(self, quadros: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        """Agrupa no máximo um quadro por source_id dentro da janela temporal."""
        por_sessao: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for quadro in quadros:
            capturas = [
                self._captura_ms(mensagem)
                for valor in quadro["mensagens"].values()
                for mensagem in (valor if isinstance(valor, list) else [valor])
            ]
            quadro["timestamp_ms"] = min(capturas) if capturas else 0
            por_sessao[quadro["session_id"]].append(quadro)
        batches: list[list[dict[str, Any]]] = []
        for quadros_sessao in por_sessao.values():
            pendentes = sorted(quadros_sessao, key=lambda q: (q["timestamp_ms"], q["source_id"], q["frame_index"]))
            while pendentes:
                base = pendentes.pop(0)
                batch = [base]
                fontes = {base["source_id"]}
                restantes: list[dict[str, Any]] = []
                for quadro in pendentes:
                    delta = quadro["timestamp_ms"] - base["timestamp_ms"]
                    if delta <= self.time_sync_threshold_ms and quadro["source_id"] not in fontes:
                        batch.append(quadro)
                        fontes.add(quadro["source_id"])
                    else:
                        restantes.append(quadro)
                pendentes = restantes
                batches.append(sorted(batch, key=lambda q: (q["timestamp_ms"], q["source_id"])))
        return sorted(batches, key=lambda b: min(q["timestamp_ms"] for q in b))

    def _processar_batch(
        self,
        batch: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        observacoes: list[dict[str, Any]] = []
        session_id_batch = batch[0]["session_id"]
        self._expirar_rastros(session_id_batch, timestamp_ms)
        message_ids: list[str] = []

        for quadro in batch:
            msgs = quadro["mensagens"]
            if "ods.i2.tracks" not in msgs:
                # S1/I9 sem I2 não basta para inventar uma identidade.
                continue

            msg_tracks = msgs["ods.i2.tracks"]
            payload_tracks = _payload(msg_tracks)
            tracks = payload_tracks.get("tracks")
            if not isinstance(tracks, list):
                raise ValueError("ods.i2.tracks exige payload.tracks como array")

            msg_posicao = msgs.get("ods.s1.position")
            posicoes: dict[str, dict[str, Any]] = {}
            if msg_posicao:
                lista_posicoes = _payload(msg_posicao).get("positions")
                if not isinstance(lista_posicoes, list):
                    raise ValueError("ods.s1.position exige payload.positions como array")
                posicoes = {
                    str(p["track_id"]): p
                    for p in lista_posicoes
                    if isinstance(p, dict) and "track_id" in p
                }

            # Organiza as avaliações do I9 por detection_id.
            mensagens_ppe = msgs.get("ods.i9.ppe_compliance", [])
            if not isinstance(mensagens_ppe, list):
                mensagens_ppe = [mensagens_ppe]

            ppe_por_detection: dict[int, list[dict[str, Any]]] = {}

            for msg_ppe in mensagens_ppe:
                ppe_payload = _payload(msg_ppe)
                uncertain_payload = ppe_payload.get("uncertain", False)

                if not isinstance(uncertain_payload, bool):
                    raise ValueError(
                        "ods.i9.ppe_compliance exige payload.uncertain boolean"
                    )

                def normalizar_itens(
                    itens: Any,
                    uncertain: bool,
                ) -> list[dict[str, Any]]:
                    if not isinstance(itens, list):
                        raise ValueError(
                            "Avaliações de EPI do I9 devem ser um array"
                        )

                    normalizados: list[dict[str, Any]] = []
                    for item in itens:
                        if not isinstance(item, dict):
                            continue

                        uncertain_item = item.get("uncertain", uncertain)
                        if not isinstance(uncertain_item, bool):
                            raise ValueError(
                                "O campo uncertain de uma avaliação de EPI deve ser boolean"
                            )

                        normalizados.append({
                            **item,
                            "uncertain": uncertain_item,
                        })

                    return normalizados

                # Formato agregado: vários detection_id na mesma mensagem.
                for registro in ppe_payload.get("items_by_detection", []):
                    if (
                        isinstance(registro, dict)
                        and isinstance(registro.get("detection_id"), int)
                        and not isinstance(registro.get("detection_id"), bool)
                    ):
                        uncertain_registro = registro.get(
                            "uncertain", uncertain_payload
                        )
                        if not isinstance(uncertain_registro, bool):
                            raise ValueError(
                                "O campo uncertain do registro I9 deve ser boolean"
                            )

                        ppe_por_detection[registro["detection_id"]] = normalizar_itens(
                            registro.get("items", []),
                            uncertain_registro,
                        )

                # Formato normativo: uma mensagem por detection_id.
                detection_id = ppe_payload.get("detection_id")
                if (
                    isinstance(detection_id, int)
                    and not isinstance(detection_id, bool)
                ):
                    ppe_por_detection[detection_id] = normalizar_itens(
                        ppe_payload.get("items", []),
                        uncertain_payload,
                    )

                # Compatibilidade com fixtures agregadas de testes.
                for registro in ppe_payload.get("evaluations", []):
                    if (
                        isinstance(registro, dict)
                        and isinstance(registro.get("detection_id"), int)
                        and not isinstance(registro.get("detection_id"), bool)
                    ):
                        uncertain_registro = registro.get(
                            "uncertain", uncertain_payload
                        )
                        if not isinstance(uncertain_registro, bool):
                            raise ValueError(
                                "O campo uncertain da avaliação I9 deve ser boolean"
                            )

                        ppe_por_detection[registro["detection_id"]] = normalizar_itens(
                            registro.get("items", []),
                            uncertain_registro,
                        )

            frame_index = quadro["frame_index"]
            session_id = quadro["session_id"]
            source_id = quadro["source_id"]

            for valor in msgs.values():
                lista_mensagens = valor if isinstance(valor, list) else [valor]
                message_ids.extend(
                    m.get("message_id", "") for m in lista_mensagens
                )

            for track in tracks:
                if not isinstance(track, dict) or not track.get("track_id"):
                    continue

                track_id = str(track["track_id"])
                state = track.get("state", "CONFIRMED")

                if state == "LOST":
                    continue

                predicted = track.get("predicted", False)
                if not isinstance(predicted, bool):
                    raise ValueError(
                        "ods.i2.tracks exige tracks[].predicted boolean quando informado"
                    )

                pos = posicoes.get(track_id, {})
                point_px = pos.get("point_px", track.get("point_px"))

                if (
                    point_px is None
                    and isinstance(track.get("bbox"), list)
                    and len(track["bbox"]) == 4
                ):
                    x1, y1, x2, y2 = track["bbox"]
                    point_px = [
                        round((x1 + x2) / 2),
                        int(y2),
                    ]

                key_rastro = (session_id, source_id, track_id)
                detection_id = track.get("detection_id")

                # Uma previsão não deve atualizar a avaliação de EPI.
                # Somente uma detecção real com avaliação correspondente
                # atualiza o estado de PPE armazenado para o rastro.
                if (
                    not predicted
                    and isinstance(detection_id, int)
                    and not isinstance(detection_id, bool)
                    and detection_id in ppe_por_detection
                ):
                    itens_validos: dict[str, dict[str, Any]] = {}

                    for item_ppe in ppe_por_detection[detection_id]:
                        if (
                            isinstance(item_ppe, dict)
                            and item_ppe.get("equipment_item")
                        ):
                            equipamento = str(item_ppe["equipment_item"])

                            itens_validos[equipamento] = {
                                "equipment_item": equipamento,
                                "compliance": item_ppe.get("compliance"),
                                "confidence": float(
                                    item_ppe.get("confidence", 0.0)
                                ),
                                "uncertain": item_ppe.get("uncertain", False),
                            }

                    self._ppe_por_rastro[key_rastro] = itens_validos

                self.atualizar_estado_memoria({
                    "session_id": session_id,
                    "source_id": source_id,
                    **track,
                })

                self._ultima_observacao_rastro[key_rastro] = self._captura_ms(
                    msg_tracks
                )

                # Durante predicted, não expõe a avaliação de EPI anterior
                # como se fosse uma observação atual.
                ppe_observado = (
                    []
                    if predicted
                    else list(
                        self._ppe_por_rastro.get(key_rastro, {}).values()
                    )
                )

                observacoes.append({
                    "session_id": session_id,
                    "source_id": source_id,
                    "track_id": track_id,
                    "track_key": key_rastro,
                    "global_id": self._entity_para_rastro(
                        session_id, source_id, track_id
                    ),
                    "class": track.get("class"),
                    "confidence": float(track.get("confidence", 1.0)),
                    "frame_index": frame_index,
                    "timestamp_ms": self._captura_ms(msg_tracks),
                    "point_px": point_px,
                    "position_m": pos.get("position_m"),
                    "space_id": pos.get("space_id"),
                    "zone_ids": list(pos.get("zone_ids", [])),
                    "ppe": ppe_observado,
                    "predicted": predicted,
                    "message_id": msg_tracks["message_id"],
                })

        clusters = self._clusterizar_observacoes(observacoes, timestamp_ms)
        entidades = [
            self._montar_entidade(cluster)
            for cluster in clusters
        ]
        self._atualizar_historico_entidades(
            entidades, observacoes, timestamp_ms
        )

        session_id = batch[0]["session_id"]
        fontes = sorted({o["source_id"] for o in observacoes})
        fonte_principal = fontes[0] if fontes else batch[0]["source_id"] or None

        msg_out = self._envelope_evento(
            "ods.s2.entity",
            session_id,
            timestamp_ms,
            fonte_principal,
            ARTIFACT_VERSION_FUSAO,
            message_ids[0] if message_ids else None,
        )
        msg_out["payload"] = {"entities": entidades}

        return msg_out, entidades, observacoes

    def _clusterizar_observacoes(
        self,
        observacoes: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> list[list[dict[str, Any]]]:
        clusters: list[list[dict[str, Any]]] = []

        for obs in sorted(
            observacoes,
            key=lambda o: (
                o["timestamp_ms"],
                o["source_id"],
                o["track_id"],
            ),
        ):
            encaixe: list[dict[str, Any]] | None = None

            if obs.get("position_m") is not None and obs.get("space_id"):
                for cluster in clusters:
                    if any(
                        item["source_id"] == obs["source_id"]
                        for item in cluster
                    ):
                        continue

                    referencia = next(
                        (
                            item
                            for item in cluster
                            if item.get("position_m") is not None
                            and item.get("space_id") == obs["space_id"]
                        ),
                        None,
                    )

                    if referencia is None:
                        continue

                    delta_t = abs(
                        obs["timestamp_ms"] - referencia["timestamp_ms"]
                    )
                    if delta_t > self.time_sync_threshold_ms:
                        continue

                    x1, y1 = obs["position_m"]
                    x2, y2 = referencia["position_m"]

                    if (
                        math.hypot(
                            float(x1) - float(x2),
                            float(y1) - float(y2),
                        )
                        <= self.merge_distance_threshold
                    ):
                        encaixe = cluster
                        break

            if encaixe is None:
                clusters.append([obs])
            else:
                encaixe.append(obs)

        for cluster in clusters:
            if len(cluster) > 1:
                ids = sorted({
                    obs["global_id"]
                    for obs in cluster
                })
                canonical = ids[0]

                for obs in cluster:
                    obs["global_id"] = canonical
                    self._track_to_entity[obs["track_key"]] = canonical

        ids_ativos = {
            obs["global_id"]
            for cluster in clusters
            for obs in cluster
        }
        ids_reutilizados: set[str] = set()

        for cluster in clusters:
            ids_cluster = {
                obs["global_id"]
                for obs in cluster
            }

            if any(
                entity_id in self._estado_entidades
                for entity_id in ids_cluster
            ):
                continue

            melhor: tuple[float, str] | None = None
            session_id_cluster = (
                cluster[0]["session_id"] if cluster else None
            )

            for entity_id, anterior in self._estado_entidades.items():
                if anterior.get("session_id") != session_id_cluster:
                    continue
                if entity_id in ids_ativos or entity_id in ids_reutilizados:
                    continue

                delta_t = timestamp_ms - int(anterior["last_seen_ms"])
                if delta_t < 0 or delta_t > self.ttl_threshold_ms:
                    continue

                for obs in cluster:
                    if (
                        anterior.get("class")
                        and obs.get("class") != anterior.get("class")
                    ):
                        continue

                    distancia: float | None = None

                    if (
                        obs.get("position_m") is not None
                        and anterior.get("position_m") is not None
                        and obs.get("space_id") == anterior.get("space_id")
                    ):
                        a, b = obs["position_m"], anterior["position_m"]
                        distancia = math.hypot(
                            float(a[0]) - float(b[0]),
                            float(a[1]) - float(b[1]),
                        )
                        limite = self.merge_distance_threshold

                    elif (
                        obs.get("point_px") is not None
                        and anterior.get("point_px") is not None
                        and obs.get("source_id") == anterior.get("source_id")
                        and not obs.get("position_m")
                        and not anterior.get("position_m")
                    ):
                        a, b = obs["point_px"], anterior["point_px"]
                        distancia = math.hypot(
                            float(a[0]) - float(b[0]),
                            float(a[1]) - float(b[1]),
                        )
                        limite = float(REIDENTIFICACAO_PIXELS)

                    else:
                        continue

                    if distancia <= limite and (
                        melhor is None or distancia < melhor[0]
                    ):
                        melhor = (distancia, entity_id)

            if melhor is not None:
                entity_id = melhor[1]
                ids_reutilizados.add(entity_id)

                for obs in cluster:
                    obs["global_id"] = entity_id
                    self._track_to_entity[obs["track_key"]] = entity_id

        return clusters

    def _atualizar_historico_entidades(
        self,
        entidades: list[dict[str, Any]],
        observacoes: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> None:
        por_id: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for obs in observacoes:
            por_id[obs["global_id"]].append(obs)

        for entidade in entidades:
            entity_id = entidade["entity_id"]
            obs = por_id.get(entity_id, [])
            referencia = obs[0] if obs else {}

            self._estado_entidades[entity_id] = {
                "session_id": (
                    obs[0].get("session_id") if obs else None
                ),
                "last_seen_ms": timestamp_ms,
                "class": entidade.get("class"),
                "position_m": entidade.get("position_m"),
                "space_id": entidade.get("space_id"),
                "point_px": entidade.get("point_px"),
                "source_id": referencia.get("source_id"),
                "zone_ids": entidade.get("zone_ids", []),
            }

    def _montar_entidade(
        self,
        cluster: list[dict[str, Any]],
    ) -> dict[str, Any]:
        fontes = sorted({
            obs["source_id"]
            for obs in cluster
        })
        zonas = sorted({
            zona
            for obs in cluster
            for zona in obs.get("zone_ids", [])
        })

        entidade: dict[str, Any] = {
            "entity_id": sorted(
                obs["global_id"] for obs in cluster
            )[0],
            "class": next(
                (
                    obs["class"]
                    for obs in cluster
                    if obs.get("class")
                ),
                "unknown",
            ),
            "zone_ids": zonas,
            "source_ids": fontes,
            "track_refs": [
                {
                    "source_id": obs["source_id"],
                    "track_id": obs["track_id"],
                }
                for obs in sorted(
                    cluster,
                    key=lambda o: (
                        o["source_id"],
                        o["track_id"],
                    ),
                )
            ],
        }

        metricas = [
            obs
            for obs in cluster
            if obs.get("position_m") is not None
            and obs.get("space_id")
        ]
        spaces = {
            obs["space_id"]
            for obs in metricas
        }

        if metricas and len(spaces) == 1:
            entidade["position_m"] = [
                round(
                    sum(
                        float(obs["position_m"][i])
                        for obs in metricas
                    ) / len(metricas),
                    3,
                )
                for i in (0, 1)
            ]
            entidade["space_id"] = next(iter(spaces))

        elif len(fontes) == 1:
            point = next(
                (
                    obs.get("point_px")
                    for obs in cluster
                    if obs.get("point_px") is not None
                ),
                None,
            )
            if point is not None:
                entidade["point_px"] = [
                    int(round(float(point[0]))),
                    int(round(float(point[1]))),
                ]

        if len(fontes) > 1 and "position_m" not in entidade:
            entidade.pop("point_px", None)

        # Consolida avaliações de EPI sem descartar o campo uncertain.
        ppe: dict[str, dict[str, Any]] = {}

        for obs in sorted(
            cluster,
            key=lambda o: o["timestamp_ms"],
        ):
            for item in obs.get("ppe", []):
                equipamento = item.get("equipment_item")

                if equipamento:
                    ppe[str(equipamento)] = {
                        "equipment_item": str(equipamento),
                        "compliance": item.get("compliance"),
                        "confidence": float(
                            item.get("confidence", 0.0)
                        ),
                        "uncertain": item.get("uncertain", False) is True,
                    }

        if ppe:
            entidade["ppe"] = list(ppe.values())

        return entidade

    def _avaliar_regras_por_entidade(
        self,
        entidades: list[dict[str, Any]],
        observacoes: list[dict[str, Any]],
        batch: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> list[dict[str, Any]]:
        saida: list[dict[str, Any]] = []
        observacoes_por_entity: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for obs in observacoes:
            observacoes_por_entity[obs["global_id"]].append(obs)

        session_id = batch[0]["session_id"]

        for entidade in entidades:
            entity_id = entidade["entity_id"]
            obs_entity = observacoes_por_entity.get(entity_id, [])
            self._ultima_observacao_entidade[(session_id, entity_id)] = timestamp_ms

            for regra in self.regras:
                tipo = regra["event_type"]
                classe_ok = (
                    not regra.get("class")
                    or entidade.get("class") == regra.get("class")
                )
                zona_regra = regra.get("zone_id")
                zonas = entidade.get("zone_ids", [])
                zona_ok = not zona_regra or zona_regra in zonas

                context_base = {
                    "entity_id": entity_id,
                    "class": entidade.get("class"),
                    "zone_id": (
                        zona_regra
                        or (zonas[0] if len(zonas) == 1 else None)
                    ),
                    "source_ids": entidade.get("source_ids", []),
                    "message_id": (
                        obs_entity[0]["message_id"]
                        if obs_entity else None
                    ),
                    "confidence": min(
                        (
                            obs["confidence"]
                            for obs in obs_entity
                        ),
                        default=1.0,
                    ),
                }

                if tipo == "PPE_NON_COMPLIANT":
                    equipamento_regra = regra.get("equipment_item")
                    ppe_by_item = {
                        i.get("equipment_item"): i
                        for i in entidade.get("ppe", [])
                    }

                    equipamentos = (
                        [equipamento_regra]
                        if equipamento_regra
                        else list(ppe_by_item)
                    )

                    for equipamento in equipamentos:
                        ppe = ppe_by_item.get(equipamento)

                        if ppe is None:
                            # Sem observação de I9 não se infere
                            # conformidade nem ausência.
                            continue

                        compliance = ppe.get("compliance")
                        condicao = (
                            compliance in {"ABSENT", "INCORRECT"}
                            and classe_ok
                            and zona_ok
                        )

                        confidence = float(
                            ppe.get("confidence", 0.0)
                        )
                        uncertain = ppe.get("uncertain") is True

                        contexto = {
                            **context_base,
                            "equipment_item": equipamento,
                            "confidence": confidence,
                            "uncertain": uncertain,
                            "details": {
                                "compliance": compliance,
                                "min_duration_ms": regra["min_duration_ms"],
                                "uncertain": uncertain,
                            },
                        }

                        saida.extend(
                            self._atualizar_condicao(
                                regra,
                                (entity_id, equipamento),
                                condicao,
                                timestamp_ms,
                                contexto,
                                session_id,
                            )
                        )

                elif tipo == "DWELL_EXCEEDED":
                    if not classe_ok:
                        continue

                    zonas_avaliadas = (
                        [zona_regra]
                        if zona_regra
                        else zonas
                    )

                    for zona in zonas_avaliadas:
                        if not zona:
                            continue

                        chave_zona = (
                            session_id,
                            f"{entity_id}:{zona}",
                        )

                        if zona in zonas:
                            inicio = self._inicio_zona.setdefault(
                                chave_zona,
                                timestamp_ms,
                            )
                            threshold_ms = int(
                                float(regra.get("threshold", 0)) * 1000
                            )
                            duracao_zona = max(
                                0,
                                timestamp_ms - inicio,
                            )
                            condicao = duracao_zona >= threshold_ms

                        else:
                            condicao = False
                            inicio = timestamp_ms
                            duracao_zona = 0
                            self._inicio_zona.pop(chave_zona, None)

                        contexto = {
                            **context_base,
                            "zone_id": zona,
                            "details": {
                                "dwell_s": int(duracao_zona // 1000)
                            },
                        }

                        saida.extend(
                            self._atualizar_condicao(
                                regra,
                                (entity_id, zona),
                                condicao,
                                timestamp_ms,
                                contexto,
                                session_id,
                            )
                        )

                elif tipo == "OBJECT_STATIONARY":
                    if not classe_ok or not zona_ok:
                        continue

                    localizacao = (
                        entidade.get("position_m")
                        or entidade.get("point_px")
                    )

                    if not localizacao or regra.get("threshold") is None:
                        continue

                    unidade = (
                        "m"
                        if entidade.get("position_m")
                        else "px"
                    )
                    key = (session_id, entity_id)
                    atual_x = float(localizacao[0])
                    atual_y = float(localizacao[1])
                    base = self._posicao_base.get(key)

                    if base is None or base[3] != unidade:
                        self._posicao_base[key] = (
                            timestamp_ms,
                            atual_x,
                            atual_y,
                            unidade,
                        )
                        deslocamento = 0.0
                        duracao = 0

                    else:
                        inicio, x0, y0, _ = base
                        deslocamento = math.hypot(
                            atual_x - x0,
                            atual_y - y0,
                        )
                        duracao = max(
                            0,
                            timestamp_ms - inicio,
                        )

                        if deslocamento > float(regra["threshold"]):
                            self._posicao_base[key] = (
                                timestamp_ms,
                                atual_x,
                                atual_y,
                                unidade,
                            )
                            duracao = 0

                    condicao = (
                        deslocamento <= float(regra["threshold"])
                        and duracao >= int(regra["min_duration_ms"])
                    )

                    contexto = {
                        **context_base,
                        "details": {
                            f"displacement_{unidade}": round(
                                deslocamento, 3
                            )
                        },
                    }

                    saida.extend(
                        self._atualizar_condicao(
                            regra,
                            (entity_id,),
                            condicao,
                            timestamp_ms,
                            contexto,
                            session_id,
                            persistencia_override_ms=0,
                        )
                    )

                elif tipo == "CROSS_CAMERA_MATCH":
                    condicao = (
                        classe_ok
                        and len(entidade.get("source_ids", [])) >= 2
                    )

                    chave_condicao = (
                        session_id,
                        regra["rule_id"],
                        entity_id,
                    )
                    estado_match = self._condicoes.get(chave_condicao)

                    dentro_ttl = bool(
                        estado_match
                        and timestamp_ms
                        - int(
                            estado_match.get(
                                "last_seen_ms",
                                timestamp_ms,
                            )
                        )
                        <= self.ttl_threshold_ms
                    )

                    if condicao or not dentro_ttl:
                        contexto = {
                            **context_base,
                            "confidence": context_base["confidence"],
                            "details": {
                                "source_ids": entidade.get(
                                    "source_ids", []
                                )
                            },
                        }

                        saida.extend(
                            self._atualizar_condicao(
                                regra,
                                (entity_id,),
                                condicao,
                                timestamp_ms,
                                contexto,
                                session_id,
                            )
                        )

        return saida

    def _atualizar_condicao(
        self,
        regra: dict[str, Any],
        identidade: tuple[Any, ...],
        ativa: bool,
        timestamp_ms: int,
        contexto: dict[str, Any],
        session_id: str,
        persistencia_override_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        rule_id = regra["rule_id"]
        chave = (session_id, rule_id, *identidade)
        saida: list[dict[str, Any]] = []

        if not ativa:
            estado = self._condicoes.get(chave)

            if estado and estado.get("event_id"):
                saida.append(
                    self._emitir_evento(
                        regra,
                        estado,
                        "ENDED",
                        timestamp_ms,
                        session_id,
                        contexto,
                    )
                )

            self._condicoes.pop(chave, None)
            self.filtro_temporal.atualizar_persistencia(
                chave, False, timestamp_ms, 0
            )
            return saida

        confidence = float(
            contexto.get("confidence", 1.0)
        )

        if confidence < float(regra["min_confidence"]):
            return []

        estado = self._condicoes.get(chave)

        if estado is None:
            estado = {
                "first_seen_ms": timestamp_ms,
                "last_seen_ms": timestamp_ms,
                "last_emitted_ms": None,
                "event_id": None,
                "contexto": dict(contexto),
            }
            self._condicoes[chave] = estado

        else:
            estado["last_seen_ms"] = timestamp_ms
            estado["contexto"] = dict(contexto)

        min_duration = int(
            regra["min_duration_ms"]
            if persistencia_override_ms is None
            else persistencia_override_ms
        )

        duracao = max(
            0,
            timestamp_ms - int(estado["first_seen_ms"]),
        )

        persistiu, _ = self.filtro_temporal.atualizar_persistencia(
            chave,
            True,
            timestamp_ms,
            min_duration,
        )

        if not estado.get("event_id") and persistiu:
            if self.filtro_temporal.aplicar_debounce(
                chave,
                timestamp_ms,
                int(regra["debounce_ms"]),
            ):
                estado["event_id"] = str(uuid.uuid4())
                estado["last_emitted_ms"] = timestamp_ms

                saida.append(
                    self._emitir_evento(
                        regra,
                        estado,
                        "STARTED",
                        timestamp_ms,
                        session_id,
                        contexto,
                    )
                )

        elif estado.get("event_id"):
            debounce_ms = int(regra["debounce_ms"])

            if (
                estado["last_emitted_ms"] is None
                or timestamp_ms - estado["last_emitted_ms"] >= debounce_ms
            ):
                if self.filtro_temporal.aplicar_debounce(
                    chave,
                    timestamp_ms,
                    debounce_ms,
                ):
                    estado["last_emitted_ms"] = timestamp_ms

                    saida.append(
                        self._emitir_evento(
                            regra,
                            estado,
                            "UPDATED",
                            timestamp_ms,
                            session_id,
                            contexto,
                        )
                    )

        return saida

    def _emitir_evento(
        self,
        regra: dict[str, Any],
        estado: dict[str, Any],
        phase: str,
        timestamp_ms: int,
        session_id: str,
        contexto: dict[str, Any],
    ) -> dict[str, Any]:
        confidence = min(
            1.0,
            max(0.0, float(contexto.get("confidence", 1.0))),
        )

        # Uma avaliação explicitamente incerta pelo I9 deve continuar
        # marcada como UNCERTAIN, mesmo que sua confiança seja alta.
        status = (
            "UNCERTAIN"
            if (
                contexto.get("uncertain") is True
                or confidence < float(regra["uncertain_below"])
            )
            else "CONFIRMED"
        )

        fontes = sorted(
            set(contexto.get("source_ids") or [])
        )
        fonte = fontes[0] if fontes else None

        msg = self._envelope_evento(
            "ods.s2.event",
            session_id,
            timestamp_ms,
            fonte,
            self.artifact_version_regras,
            contexto.get("message_id"),
        )

        payload: dict[str, Any] = {
            "event_id": estado["event_id"],
            "event_type": regra["event_type"],
            "rule_id": regra["rule_id"],
            "status": status,
            "phase": phase,
            "confidence": round(confidence, 4),
        }

        for campo in (
            "entity_id",
            "class",
            "zone_id",
            "equipment_item",
        ):
            if contexto.get(campo) is not None:
                payload[campo] = contexto[campo]

        payload["started_at"] = _iso_de_ms(
            int(estado["first_seen_ms"])
        )
        payload["duration_ms"] = max(
            0,
            timestamp_ms - int(estado["first_seen_ms"]),
        )
        payload["source_ids"] = fontes

        details = contexto.get("details")
        if isinstance(details, dict) and details:
            payload["details"] = details

        msg["payload"] = payload
        return msg

    def _processar_transicao_zona(
        self,
        mensagem: dict[str, Any],
        timestamp_ms: int,
    ) -> list[dict[str, Any]]:
        payload = _payload(mensagem)
        transicao = payload.get("transition")
        zona = payload.get("zone_id")
        source_id = mensagem.get("source_id")
        session_id = mensagem["session_id"]
        track_id = str(payload.get("track_id", ""))

        if transicao not in {"ENTER", "EXIT"} or not zona or not track_id:
            raise ValueError(
                "ods.s1.zone_transition exige track_id, zone_id e transition ENTER/EXIT"
            )

        entity_id = self._entity_para_rastro(
            session_id,
            str(source_id or ""),
            track_id,
        )
        fontes = [str(source_id)] if source_id else []
        saida: list[dict[str, Any]] = []
        classe = payload.get("class")

        for regra in self.regras:
            tipo = regra["event_type"]

            if tipo not in {"ZONE_ENTER", "ZONE_EXIT"}:
                continue
            if regra.get("class") and regra["class"] != classe:
                continue
            if regra.get("zone_id") and regra["zone_id"] != zona:
                continue

            contexto = {
                "entity_id": entity_id,
                "class": classe,
                "zone_id": zona,
                "source_ids": fontes,
                "message_id": mensagem["message_id"],
                "confidence": 1.0,
                "details": {},
            }

            if tipo == "ZONE_ENTER" and transicao == "ENTER":
                saida.extend(
                    self._atualizar_condicao(
                        regra,
                        (entity_id, zona),
                        True,
                        timestamp_ms,
                        contexto,
                        session_id,
                        persistencia_override_ms=0,
                    )
                )

            elif tipo == "ZONE_ENTER" and transicao == "EXIT":
                saida.extend(
                    self._atualizar_condicao(
                        regra,
                        (entity_id, zona),
                        False,
                        timestamp_ms,
                        contexto,
                        session_id,
                    )
                )

            elif tipo == "ZONE_EXIT" and transicao == "EXIT":
                eventos_start = self._atualizar_condicao(
                    regra,
                    (entity_id, zona),
                    True,
                    timestamp_ms,
                    contexto,
                    session_id,
                    persistencia_override_ms=0,
                )
                saida.extend(eventos_start)

                chave = (
                    session_id,
                    regra["rule_id"],
                    entity_id,
                    zona,
                )
                estado = self._condicoes.get(chave)

                if estado and estado.get("event_id"):
                    saida.append(
                        self._emitir_evento(
                            regra,
                            estado,
                            "ENDED",
                            timestamp_ms,
                            session_id,
                            contexto,
                        )
                    )

                self._condicoes.pop(chave, None)

        return saida

    def _processar_ocupacao(
        self,
        mensagem: dict[str, Any],
        timestamp_ms: int,
    ) -> list[dict[str, Any]]:
        payload = _payload(mensagem)
        zona = payload.get("zone_id")
        count = payload.get("count")

        if (
            not isinstance(zona, str)
            or not isinstance(count, int)
            or isinstance(count, bool)
        ):
            raise ValueError(
                "ods.s1.zone_occupancy exige zone_id string e count integer"
            )

        session_id = mensagem["session_id"]
        fonte = mensagem.get("source_id")
        chave_zona = (session_id, zona)
        anterior = self._contagem_zonas.get(chave_zona)
        self._contagem_zonas[chave_zona] = count
        saida: list[dict[str, Any]] = []

        if anterior is None:
            return saida

        for regra in self.regras:
            tipo = regra["event_type"]
            threshold = regra.get("threshold")

            if tipo not in {"OCCUPANCY_ABOVE", "OCCUPANCY_BELOW"} or threshold is None:
                continue
            if regra.get("zone_id") and regra["zone_id"] != zona:
                continue

            cruzou = (
                tipo == "OCCUPANCY_ABOVE"
                and anterior <= threshold < count
            ) or (
                tipo == "OCCUPANCY_BELOW"
                and anterior >= threshold > count
            )

            contexto = {
                "class": regra.get("class"),
                "zone_id": zona,
                "source_ids": [fonte] if fonte else [],
                "message_id": mensagem["message_id"],
                "confidence": 1.0,
                "details": {"count": count},
            }

            chave = (zona,)

            if cruzou:
                eventos_start = self._atualizar_condicao(
                    regra,
                    chave,
                    True,
                    timestamp_ms,
                    contexto,
                    session_id,
                    persistencia_override_ms=0,
                )
                saida.extend(eventos_start)

                estado_key = (
                    session_id,
                    regra["rule_id"],
                    *chave,
                )
                estado = self._condicoes.get(estado_key)

                if estado and estado.get("event_id"):
                    saida.append(
                        self._emitir_evento(
                            regra,
                            estado,
                            "ENDED",
                            timestamp_ms,
                            session_id,
                            contexto,
                        )
                    )

                self._condicoes.pop(estado_key, None)

        return saida

    def _encerrar_condicoes_de_entidades_ausentes(
        self,
        entidades: list[dict[str, Any]],
        timestamp_ms: int,
        session_id: str,
        saida: list[dict[str, Any]],
    ) -> None:
        presentes = {
            e["entity_id"]
            for e in entidades
        }

        for chave, estado in list(self._condicoes.items()):
            if chave[0] != session_id or estado.get("event_id") is None:
                continue

            entity_id = chave[2] if len(chave) >= 3 else None

            if (
                entity_id in presentes
                or not isinstance(entity_id, str)
                or not entity_id.startswith("e-")
            ):
                continue

            regra = next(
                (
                    r
                    for r in self.regras
                    if r["rule_id"] == chave[1]
                ),
                None,
            )

            if regra is None:
                continue

            if (
                timestamp_ms
                - int(estado.get("last_seen_ms", timestamp_ms))
                <= self.ttl_threshold_ms
            ):
                continue

            contexto = dict(
                estado.get("contexto", {})
            )

            saida.append(
                self._emitir_evento(
                    regra,
                    estado,
                    "ENDED",
                    timestamp_ms,
                    session_id,
                    contexto,
                )
            )
            self._condicoes.pop(chave, None)

    def _expirar_rastros(
        self,
        session_id: str,
        timestamp_ms: int,
    ) -> None:
        """Remove rastros sem observação por mais que o TTL configurado.

        A expiração é baseada em captured_at, não no relógio da máquina,
        para manter o comportamento reproduzível em replay e testes.
        """
        expirados = [
            chave
            for chave, ultimo in self._ultima_observacao_rastro.items()
            if (
                chave[0] == session_id
                and timestamp_ms - ultimo > self.ttl_threshold_ms
            )
        ]

        for chave in expirados:
            self._ultima_observacao_rastro.pop(chave, None)
            self._track_to_entity.pop(chave, None)
            self._ppe_por_rastro.pop(chave, None)

            chave_memoria = (
                f"{chave[0]}:{chave[1]}:{chave[2]}"
            )
            self.estado_em_memoria.pop(chave_memoria, None)

        expiradas_entidades = [
            entity_id
            for entity_id, estado in self._estado_entidades.items()
            if (
                estado.get("session_id") == session_id
                and timestamp_ms - int(estado["last_seen_ms"])
                > self.ttl_threshold_ms
            )
        ]

        for entity_id in expiradas_entidades:
            self._estado_entidades.pop(entity_id, None)

    def _resetar_estado_sessao(
        self,
        session_id: str,
    ) -> None:
        self._estado_entidades = {
            k: v
            for k, v in self._estado_entidades.items()
            if v.get("session_id") != session_id
        }

        self.estado_em_memoria = {
            chave: valor
            for chave, valor in self.estado_em_memoria.items()
            if not chave.startswith(f"{session_id}:")
        }

        self._track_to_entity = {
            k: v
            for k, v in self._track_to_entity.items()
            if k[0] != session_id
        }

        self._ppe_por_rastro = {
            k: v
            for k, v in self._ppe_por_rastro.items()
            if k[0] != session_id
        }

        self._ultima_observacao_rastro = {
            k: v
            for k, v in self._ultima_observacao_rastro.items()
            if k[0] != session_id
        }

        self._condicoes = {
            k: v
            for k, v in self._condicoes.items()
            if k[0] != session_id
        }

        self._contagem_zonas = {
            k: v
            for k, v in self._contagem_zonas.items()
            if k[0] != session_id
        }

        self._inicio_zona = {
            k: v
            for k, v in self._inicio_zona.items()
            if k[0] != session_id
        }

        self._posicao_base = {
            k: v
            for k, v in self._posicao_base.items()
            if k[0] != session_id
        }

        self._sequence_por_sessao[session_id] = 0
        self.filtro_temporal.limpar_sessao(session_id)
