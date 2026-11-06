
"""Motor do componente S2 — Regras temporais e eventos (arquitetura 6c).

Consome:
- ods.i2.tracks
- ods.s1.position
- ods.s1.zone_transition
- ods.s1.zone_occupancy
- ods.i9.ppe_compliance (quando instalado)

Publica:
- ods.s2.entity
- ods.s2.event

A identidade global é originada exclusivamente em rastros de I2.
S1 complementa os rastros com posição e zonas.
I9 é associado a I2 por source_id, session_id, frame_index e detection_id.

A fusão métrica só ocorre quando as observações compartilham space_id.
Coordenadas em pixels não são comparadas entre câmeras diferentes.
"""
from __future__ import annotations

import math
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

try:
    from .config import (
        ARTIFACT_VERSION_FUSAO,
        DISTANCIA_FUSAO_M,
        SCHEMA_VERSION,
        SINCRONISMO_FUSAO_MS,
        TTL_ENTIDADE_MS,
        REIDENTIFICACAO_PIXELS,
    )
    from .filtro_ruido_temporal import FiltroRuidoTemporal
except ImportError:
    from config import (
        ARTIFACT_VERSION_FUSAO,
        DISTANCIA_FUSAO_M,
        SCHEMA_VERSION,
        SINCRONISMO_FUSAO_MS,
        TTL_ENTIDADE_MS,
        REIDENTIFICACAO_PIXELS,
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
    "ZONE_ENTER",
    "ZONE_EXIT",
    "DWELL_EXCEEDED",
    "OCCUPANCY_ABOVE",
    "OCCUPANCY_BELOW",
    "PPE_NON_COMPLIANT",
    "OBJECT_STATIONARY",
    "CROSS_CAMERA_MATCH",
}

COMPLIANCE_VALIDOS = {"CORRECT", "INCORRECT", "ABSENT"}
ESTADOS_RASTRO_VALIDOS = {"CONFIRMED", "LOST"}


def _agora_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _iso_de_ms(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _uuid_valido(valor: Any, campo: str) -> str:
    if not isinstance(valor, str):
        raise ValueError(f"Campo {campo!r} deve ser um UUID em string")
    try:
        return str(uuid.UUID(valor))
    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"Campo {campo!r} não contém UUID válido: {valor!r}"
        ) from exc


def _payload(mensagem: dict[str, Any]) -> dict[str, Any]:
    valor = mensagem.get("payload")
    if not isinstance(valor, dict):
        raise ValueError(
            f"A mensagem {mensagem.get('schema')!r} precisa ter payload object"
        )
    return valor


def _numero_finito(valor: Any) -> bool:
    return (
        isinstance(valor, (int, float))
        and not isinstance(valor, bool)
        and math.isfinite(float(valor))
    )


def _par_numerico(valor: Any, campo: str) -> list[float] | None:
    if valor is None:
        return None

    if (
        not isinstance(valor, (list, tuple))
        or len(valor) != 2
        or not all(_numero_finito(v) for v in valor)
    ):
        raise ValueError(f"{campo} deve ser um par [x, y] de números finitos")

    return [float(valor[0]), float(valor[1])]


def _validar_lista_strings(valor: Any, campo: str) -> list[str]:
    if not isinstance(valor, list):
        raise ValueError(f"{campo} deve ser um array")

    if any(not isinstance(item, str) or not item.strip() for item in valor):
        raise ValueError(f"{campo} deve conter somente strings não vazias")

    return list(valor)


class MotorGerenciamentoEstadoConcorrente:
    """Implementa fusão de identidade e qualificação temporal de eventos."""

    def __init__(
        self,
        merge_distance_threshold: float = DISTANCIA_FUSAO_M,
        time_sync_threshold_ms: int = SINCRONISMO_FUSAO_MS,
        filtro_temporal: FiltroRuidoTemporal | None = None,
        ttl_threshold_ms: int = TTL_ENTIDADE_MS,
    ) -> None:
        if (
            not _numero_finito(merge_distance_threshold)
            or float(merge_distance_threshold) < 0
        ):
            raise ValueError(
                "merge_distance_threshold deve ser number finito >= 0"
            )

        for nome, valor in (
            ("time_sync_threshold_ms", time_sync_threshold_ms),
            ("ttl_threshold_ms", ttl_threshold_ms),
        ):
            if (
                not isinstance(valor, int)
                or isinstance(valor, bool)
                or valor < 0
            ):
                raise ValueError(f"{nome} deve ser integer >= 0")

        if not _numero_finito(REIDENTIFICACAO_PIXELS) or REIDENTIFICACAO_PIXELS < 0:
            raise ValueError("REIDENTIFICACAO_PIXELS deve ser finito e >= 0")

        self.merge_distance_threshold = float(merge_distance_threshold)
        self.time_sync_threshold_ms = time_sync_threshold_ms
        self.ttl_threshold_ms = ttl_threshold_ms
        self.filtro_temporal = filtro_temporal or FiltroRuidoTemporal()

        self.estado_em_memoria: dict[str, dict[str, Any]] = {}
        self.global_id_counter = 1
        self.artifact_version_regras: str | None = None
        self.regras: list[dict[str, Any]] = []

        # Chave local: (session_id, source_id, track_id).
        self._track_to_entity: dict[tuple[str, str, str], str] = {}
        self._ppe_por_rastro: dict[
            tuple[str, str, str], dict[str, dict[str, Any]]
        ] = {}

        # Chave de condição: (session_id, rule_id, identidade...).
        self._condicoes: dict[tuple[Any, ...], dict[str, Any]] = {}

        self._contagem_zonas: dict[tuple[str, str], int] = {}
        self._inicio_zona: dict[tuple[str, str], int] = {}
        self._posicao_base: dict[
            tuple[str, str], tuple[int, float, float, str]
        ] = {}

        self._sequence_por_sessao: dict[str, int] = defaultdict(int)
        self._processadas: set[str] = set()
        self._ultima_observacao_entidade: dict[tuple[str, str], int] = {}
        self._ultima_observacao_rastro: dict[
            tuple[str, str, str], int
        ] = {}
        self._estado_entidades: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Regras
    # ------------------------------------------------------------------

    def carregar_regras(self, artefato_regras: dict[str, Any] | None) -> None:
        """Carrega o payload do artefato RULES."""
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

        obrigatorios = (
            "rule_id",
            "event_type",
            "min_confidence",
            "uncertain_below",
            "min_duration_ms",
            "debounce_ms",
        )

        for indice, regra_original in enumerate(regras):
            if not isinstance(regra_original, dict):
                raise ValueError(f"rules[{indice}] deve ser um object")

            regra = dict(regra_original)
            faltantes = [
                campo for campo in obrigatorios if campo not in regra
            ]
            if faltantes:
                raise ValueError(
                    f"rules[{indice}] sem campos obrigatórios: "
                    f"{', '.join(faltantes)}"
                )

            rule_id = regra["rule_id"]
            if not isinstance(rule_id, str) or not rule_id.strip():
                raise ValueError(
                    f"rules[{indice}].rule_id deve ser string não vazia"
                )
            if rule_id in ids:
                raise ValueError(f"rule_id duplicado: {rule_id}")
            ids.add(rule_id)

            event_type = regra["event_type"]
            if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
                raise ValueError(f"UNKNOWN_EVENT_TYPE: {event_type!r}")

            min_conf = regra["min_confidence"]
            uncertain_below = regra["uncertain_below"]

            if not _numero_finito(min_conf) or not 0 <= min_conf <= 1:
                raise ValueError(
                    f"rules[{indice}].min_confidence deve estar entre 0 e 1"
                )

            if (
                not _numero_finito(uncertain_below)
                or not min_conf <= uncertain_below <= 1
            ):
                raise ValueError(
                    f"rules[{indice}].uncertain_below deve estar "
                    "entre min_confidence e 1"
                )

            for campo in ("min_duration_ms", "debounce_ms"):
                valor = regra[campo]
                if (
                    not isinstance(valor, int)
                    or isinstance(valor, bool)
                    or valor < 0
                ):
                    raise ValueError(
                        f"rules[{indice}].{campo} deve ser integer >= 0"
                    )

            if "threshold" in regra and not _numero_finito(regra["threshold"]):
                raise ValueError(
                    f"rules[{indice}].threshold deve ser number finito"
                )

            for campo in ("class", "zone_id", "equipment_item"):
                if campo in regra and regra[campo] is not None:
                    if not isinstance(regra[campo], str):
                        raise ValueError(
                            f"rules[{indice}].{campo} deve ser string"
                        )

            validadas.append(regra)

        self.artifact_version_regras = versao
        self.regras = validadas

    # ------------------------------------------------------------------
    # Estado e envelopes
    # ------------------------------------------------------------------

    def atualizar_estado_memoria(self, evento: dict[str, Any]) -> None:
        """Registra a última observação local sem substituir IDs globais."""
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

    def _entity_para_rastro(
        self,
        session_id: str,
        source_id: str,
        track_id: str,
    ) -> str:
        """Obtém ID global para rastro confirmado recebido de I2."""
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
            raise ValueError(
                f"Mensagem na posição {indice} não é um objeto JSON"
            )

        obrigatorios = (
            "message_type",
            "message_id",
            "schema",
            "schema_version",
            "producer",
            "published_at",
            "session_id",
            "sequence",
            "payload",
        )
        faltantes = [
            campo for campo in obrigatorios if campo not in mensagem
        ]

        if faltantes:
            raise ValueError(
                f"Mensagem na posição {indice} não segue o envelope comum "
                f"1.0; faltam: {', '.join(faltantes)}"
            )

        if mensagem["message_type"] != "event":
            raise ValueError(
                "S2 esperava message_type='event', recebeu "
                f"{mensagem['message_type']!r}"
            )

        _uuid_valido(mensagem["message_id"], "message_id")
        _uuid_valido(mensagem["session_id"], "session_id")

        for campo in ("schema", "schema_version", "producer"):
            if not isinstance(mensagem[campo], str) or not mensagem[campo].strip():
                raise ValueError(f"{campo} deve ser string não vazia")

        if mensagem["schema_version"].split(".")[0] != "1":
            raise ValueError(
                f"Versão de schema incompatível: {mensagem['schema_version']!r}"
            )

        sequence = mensagem["sequence"]
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
        ):
            raise ValueError("sequence deve ser integer >= 0")

        FiltroRuidoTemporal.timestamp_em_ms(mensagem["published_at"])

        if "captured_at" in mensagem:
            FiltroRuidoTemporal.timestamp_em_ms(mensagem["captured_at"])
        elif mensagem["schema"] != "ods.session.started":
            raise ValueError(
                f"A mensagem {mensagem['schema']!r} precisa ter captured_at"
            )

        _payload(mensagem)
        return mensagem

    # ------------------------------------------------------------------
    # Entrada principal, deduplicação e timeline
    # ------------------------------------------------------------------

    def fusao_e_deduplicacao(
        self,
        eventos_validos: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Processa mensagens de entrada e retorna ods.s2.entity/event."""
        mensagens: list[dict[str, Any]] = []
        comandos_regras: list[dict[str, Any]] = []

        for indice, bruto in enumerate(eventos_validos):
            if (
                isinstance(bruto, dict)
                and bruto.get("schema") == "ods.s2.rules.command"
            ):
                comando = self._validar_comando_regras(bruto, indice)
                if comando["message_id"] in self._processadas:
                    continue
                self._processadas.add(comando["message_id"])
                comandos_regras.append(comando)
                continue

            mensagem = self._validar_mensagem(bruto, indice)
            schema = mensagem["schema"]

            if schema not in SCHEMAS_CONSUMIDOS:
                raise ValueError(
                    f"S2 não consome o schema {schema!r}; "
                    "confira o mapa de produção/consumo da arquitetura 6c"
                )

            if mensagem["message_id"] in self._processadas:
                continue

            self._processadas.add(mensagem["message_id"])
            mensagens.append(mensagem)

        quadros: dict[tuple[str, str, int], dict[str, Any]] = {}
        auxiliares: list[dict[str, Any]] = []

        for mensagem in mensagens:
            schema = mensagem["schema"]

            if schema in SCHEMAS_POR_QUADRO:
                payload = _payload(mensagem)
                frame_index = payload.get("frame_index")

                if (
                    not isinstance(frame_index, int)
                    or isinstance(frame_index, bool)
                    or frame_index < 0
                ):
                    raise ValueError(
                        f"{schema} exige payload.frame_index integer >= 0"
                    )

                source_id = mensagem.get("source_id")
                if not isinstance(source_id, str) or not source_id.strip():
                    raise ValueError(f"{schema} exige source_id string não vazia")

                chave = (
                    mensagem["session_id"],
                    source_id,
                    frame_index,
                )

                grupo = quadros.setdefault(
                    chave,
                    {
                        "mensagens": {},
                        "session_id": chave[0],
                        "source_id": chave[1],
                        "frame_index": chave[2],
                    },
                )

                if schema == "ods.i9.ppe_compliance":
                    grupo["mensagens"].setdefault(schema, []).append(mensagem)
                else:
                    if schema in grupo["mensagens"]:
                        raise ValueError(
                            f"Mais de uma mensagem {schema} para "
                            f"source_id/frame_index {chave[1:]}; "
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
            timestamp = (
                self._captura_ms(mensagem)
                if "captured_at" in mensagem
                else FiltroRuidoTemporal.timestamp_em_ms(
                    mensagem["published_at"]
                )
            )
            timeline.append((timestamp, 1, mensagem))

        for comando in comandos_regras:
            timestamp = FiltroRuidoTemporal.timestamp_em_ms(
                comando["published_at"]
            )
            timeline.append((timestamp, -1, comando))

        timeline.sort(key=lambda item: (item[0], item[1]))

        saida: list[dict[str, Any]] = []

        for timestamp_ms, tipo, item in timeline:
            if tipo == -1:
                saida.append(self._processar_comando_regras(item))

            elif tipo == 0:
                mensagem_entidade, entidades, observacoes = (
                    self._processar_batch(item, timestamp_ms)
                )
                saida.append(mensagem_entidade)
                saida.extend(
                    self._avaliar_regras_por_entidade(
                        entidades,
                        observacoes,
                        item,
                        timestamp_ms,
                    )
                )
                self._encerrar_condicoes_de_entidades_ausentes(
                    entidades,
                    timestamp_ms,
                    item[0]["session_id"],
                    saida,
                )

            else:
                schema = item["schema"]

                if schema == "ods.session.started":
                    self._resetar_estado_sessao(item["session_id"])

                elif schema == "ods.s1.zone_transition":
                    saida.extend(
                        self._processar_transicao_zona(item, timestamp_ms)
                    )

                elif schema == "ods.s1.zone_occupancy":
                    saida.extend(
                        self._processar_ocupacao(item, timestamp_ms)
                    )

        return saida

    @staticmethod
    def _validar_comando_regras(
        mensagem: Any,
        indice: int,
    ) -> dict[str, Any]:
        """Valida o perfil CMD de ods.s2.rules.command."""
        if not isinstance(mensagem, dict):
            raise ValueError(
                f"Comando na posição {indice} não é um objeto JSON"
            )

        obrigatorios = (
            "message_type",
            "message_id",
            "schema",
            "schema_version",
            "producer",
            "published_at",
            "target",
            "payload",
        )
        faltantes = [
            campo for campo in obrigatorios if campo not in mensagem
        ]

        if faltantes:
            raise ValueError(
                "ods.s2.rules.command sem campos obrigatórios: "
                + ", ".join(faltantes)
            )

        if (
            mensagem["message_type"] != "command"
            or mensagem["schema"] != "ods.s2.rules.command"
        ):
            raise ValueError(
                "Comando de regras precisa usar message_type='command' "
                "e schema='ods.s2.rules.command'"
            )

        if mensagem["target"] != "S2":
            raise ValueError("ods.s2.rules.command exige target='S2'")

        _uuid_valido(mensagem["message_id"], "message_id")

        if (
            not isinstance(mensagem["schema_version"], str)
            or mensagem["schema_version"].split(".")[0] != "1"
        ):
            raise ValueError(
                "Versão de schema incompatível em ods.s2.rules.command"
            )

        FiltroRuidoTemporal.timestamp_em_ms(mensagem["published_at"])

        if not isinstance(mensagem["payload"], dict):
            raise ValueError(
                "ods.s2.rules.command exige payload do tipo object"
            )

        if (
            not isinstance(
                mensagem["payload"].get("artifact_version"), str
            )
            or not isinstance(mensagem["payload"].get("rules"), list)
        ):
            raise ValueError(
                "payload de ods.s2.rules.command exige "
                "artifact_version string e rules array"
            )

        return mensagem

    def _processar_comando_regras(
        self,
        comando: dict[str, Any],
    ) -> dict[str, Any]:
        """Aplica o artefato RULES e devolve uma reply."""
        payload = comando["payload"]

        try:
            self.carregar_regras(
                {
                    "artifact_version": payload["artifact_version"],
                    "rules": payload["rules"],
                }
            )
            resposta = {
                "status": "OK",
                "applied_rule_ids": [r["rule_id"] for r in self.regras],
            }
        except ValueError as exc:
            texto = str(exc)
            codigo = (
                "UNKNOWN_EVENT_TYPE"
                if "UNKNOWN_EVENT_TYPE" in texto
                else "INVALID_RULE"
            )
            resposta = {
                "status": "ERROR",
                "code": codigo,
                "message": texto,
            }

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

    def processar_mensagens(
        self,
        mensagens: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Alias de nomenclatura para chamadas novas."""
        return self.fusao_e_deduplicacao(mensagens)

    @staticmethod
    def _captura_ms(mensagem: dict[str, Any]) -> int:
        valor = mensagem.get("captured_at", mensagem.get("published_at"))
        return FiltroRuidoTemporal.timestamp_em_ms(valor)

    # ------------------------------------------------------------------
    # Agrupamento de quadros e instantes de fusão
    # ------------------------------------------------------------------

    def _agrupar_instantes_de_fusao(
        self,
        quadros: list[dict[str, Any]],
    ) -> list[list[dict[str, Any]]]:
        """Agrupa no máximo um quadro por source_id na janela temporal."""
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
            pendentes = sorted(
                quadros_sessao,
                key=lambda q: (
                    q["timestamp_ms"],
                    q["source_id"],
                    q["frame_index"],
                ),
            )

            while pendentes:
                base = pendentes.pop(0)
                batch = [base]
                fontes = {base["source_id"]}
                restantes: list[dict[str, Any]] = []

                for quadro in pendentes:
                    delta = quadro["timestamp_ms"] - base["timestamp_ms"]

                    if (
                        0 <= delta <= self.time_sync_threshold_ms
                        and quadro["source_id"] not in fontes
                    ):
                        batch.append(quadro)
                        fontes.add(quadro["source_id"])
                    else:
                        restantes.append(quadro)

                pendentes = restantes
                batches.append(
                    sorted(
                        batch,
                        key=lambda q: (q["timestamp_ms"], q["source_id"]),
                    )
                )

        return sorted(
            batches,
            key=lambda batch: min(q["timestamp_ms"] for q in batch),
        )

    # ------------------------------------------------------------------
    # Processamento de cada instante de fusão
    # ------------------------------------------------------------------

    def _processar_batch(
        self,
        batch: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        observacoes: list[dict[str, Any]] = []
        session_id_batch = batch[0]["session_id"]

        self._expirar_rastros(session_id_batch, timestamp_ms)
        message_ids: list[str] = []

        for quadro in batch:
            mensagens = quadro["mensagens"]

            # Sem I2 não há identidade que S2 possa consolidar.
            if "ods.i2.tracks" not in mensagens:
                continue

            msg_tracks = mensagens["ods.i2.tracks"]
            payload_tracks = _payload(msg_tracks)
            tracks = payload_tracks.get("tracks")

            if not isinstance(tracks, list):
                raise ValueError(
                    "ods.i2.tracks exige payload.tracks como array"
                )

            msg_posicao = mensagens.get("ods.s1.position")
            posicoes: dict[str, dict[str, Any]] = {}

            if msg_posicao:
                lista_posicoes = _payload(msg_posicao).get("positions")

                if not isinstance(lista_posicoes, list):
                    raise ValueError(
                        "ods.s1.position exige payload.positions como array"
                    )

                for posicao in lista_posicoes:
                    if not isinstance(posicao, dict):
                        raise ValueError(
                            "Cada item de positions deve ser um object"
                        )

                    track_id_pos = posicao.get("track_id")
                    if not isinstance(track_id_pos, str) or not track_id_pos:
                        raise ValueError(
                            "positions[].track_id deve ser string não vazia"
                        )

                    if track_id_pos in posicoes:
                        raise ValueError(
                            f"track_id duplicado em ods.s1.position: "
                            f"{track_id_pos}"
                        )

                    posicoes[track_id_pos] = posicao

            mensagens_ppe = mensagens.get("ods.i9.ppe_compliance", [])
            if not isinstance(mensagens_ppe, list):
                mensagens_ppe = [mensagens_ppe]

            # I9 normativo: uma mensagem por pessoa/detection_id.
            ppe_por_detection: dict[int, list[dict[str, Any]]] = {}

            for msg_ppe in mensagens_ppe:
                if (
                    msg_ppe.get("source_id") != quadro["source_id"]
                    or msg_ppe.get("session_id") != quadro["session_id"]
                ):
                    raise ValueError(
                        "I9 precisa pertencer à mesma source_id e session_id "
                        "do quadro agrupado"
                    )

                payload_ppe = _payload(msg_ppe)
                detection_id_ppe = payload_ppe.get("detection_id")

                if (
                    not isinstance(detection_id_ppe, int)
                    or isinstance(detection_id_ppe, bool)
                    or detection_id_ppe < 0
                ):
                    raise ValueError(
                        "ods.i9.ppe_compliance exige detection_id "
                        "integer >= 0"
                    )

                uncertain_payload = payload_ppe.get("uncertain")
                if not isinstance(uncertain_payload, bool):
                    raise ValueError(
                        "ods.i9.ppe_compliance exige payload.uncertain boolean"
                    )

                if detection_id_ppe in ppe_por_detection:
                    raise ValueError(
                        "Mais de uma mensagem I9 para o mesmo detection_id "
                        "no mesmo quadro"
                    )

                itens = payload_ppe.get("items")
                if not isinstance(itens, list):
                    raise ValueError(
                        "ods.i9.ppe_compliance exige payload.items array"
                    )

                itens_normalizados: list[dict[str, Any]] = []

                for item in itens:
                    if not isinstance(item, dict):
                        raise ValueError(
                            "Cada item de I9 deve ser um object"
                        )

                    equipamento = item.get("equipment_item")
                    compliance = item.get("compliance")
                    confidence = item.get("confidence")

                    if (
                        not isinstance(equipamento, str)
                        or not equipamento.strip()
                    ):
                        raise ValueError(
                            "items[].equipment_item deve ser string não vazia"
                        )

                    if compliance not in COMPLIANCE_VALIDOS:
                        raise ValueError(
                            f"Compliance inválido para {equipamento!r}: "
                            f"{compliance!r}"
                        )

                    if not _numero_finito(confidence) or not 0 <= confidence <= 1:
                        raise ValueError(
                            "items[].confidence deve ser number entre 0 e 1"
                        )

                    itens_normalizados.append(
                        {
                            "equipment_item": equipamento,
                            "compliance": compliance,
                            "confidence": float(confidence),
                            "uncertain": uncertain_payload,
                        }
                    )

                ppe_por_detection[detection_id_ppe] = itens_normalizados

            session_id = quadro["session_id"]
            source_id = quadro["source_id"]
            frame_index = quadro["frame_index"]

            for valor in mensagens.values():
                lista = valor if isinstance(valor, list) else [valor]
                message_ids.extend(
                    mensagem["message_id"] for mensagem in lista
                )

            track_ids_no_quadro: set[str] = set()

            for track in tracks:
                if not isinstance(track, dict):
                    raise ValueError(
                        "Cada item de tracks deve ser um object"
                    )

                track_id_valor = track.get("track_id")
                if not isinstance(track_id_valor, str) or not track_id_valor:
                    raise ValueError(
                        "tracks[].track_id deve ser string não vazia"
                    )

                if track_id_valor in track_ids_no_quadro:
                    raise ValueError(
                        f"track_id duplicado no quadro: {track_id_valor}"
                    )
                track_ids_no_quadro.add(track_id_valor)

                state = track.get("state")
                if state not in ESTADOS_RASTRO_VALIDOS:
                    raise ValueError(
                        f"tracks[].state inválido: {state!r}"
                    )

                predicted = track.get("predicted")
                if not isinstance(predicted, bool):
                    raise ValueError(
                        "tracks[].predicted deve ser boolean"
                    )

                track_id = track_id_valor
                key_rastro = (session_id, source_id, track_id)

                if state == "LOST":
                    self._remover_rastro_local(key_rastro)
                    continue

                detection_id = track.get("detection_id")

                if predicted:
                    if detection_id is not None:
                        raise ValueError(
                            "Um rastro predicted não deve possuir detection_id"
                        )
                elif detection_id is not None and (
                    not isinstance(detection_id, int)
                    or isinstance(detection_id, bool)
                    or detection_id < 0
                ):
                    raise ValueError(
                        "tracks[].detection_id deve ser integer >= 0"
                    )

                if not predicted and detection_id in ppe_por_detection:
                    itens_validos: dict[str, dict[str, Any]] = {}

                    for item_ppe in ppe_por_detection[detection_id]:
                        equipamento = item_ppe["equipment_item"]
                        itens_validos[equipamento] = dict(item_ppe)

                    self._ppe_por_rastro[key_rastro] = itens_validos

                posicao = posicoes.get(track_id, {})

                point_px = _par_numerico(
                    posicao.get("point_px", track.get("point_px")),
                    "point_px",
                )

                if point_px is None and track.get("bbox") is not None:
                    bbox = track["bbox"]
                    if (
                        not isinstance(bbox, list)
                        or len(bbox) != 4
                        or not all(_numero_finito(v) for v in bbox)
                    ):
                        raise ValueError(
                            "tracks[].bbox deve ser [x_min,y_min,x_max,y_max]"
                        )
                    x_min, y_min, x_max, y_max = map(float, bbox)
                    if x_max < x_min or y_max < y_min:
                        raise ValueError("tracks[].bbox possui limites inválidos")
                    point_px = [
                        (x_min + x_max) / 2,
                        y_max,
                    ]

                position_m = _par_numerico(
                    posicao.get("position_m"),
                    "position_m",
                )
                space_id = posicao.get("space_id")

                if position_m is not None:
                    if not isinstance(space_id, str) or not space_id.strip():
                        raise ValueError(
                            "position_m exige space_id string não vazia"
                        )
                elif space_id is not None:
                    raise ValueError(
                        "space_id não deve aparecer sem position_m"
                    )

                zone_ids = _validar_lista_strings(
                    posicao.get("zone_ids", []),
                    "positions[].zone_ids",
                )

                confidence = track.get("confidence")
                if not _numero_finito(confidence) or not 0 <= confidence <= 1:
                    raise ValueError(
                        "tracks[].confidence deve ser number entre 0 e 1"
                    )

                classe = track.get("class")
                if not isinstance(classe, str) or not classe.strip():
                    raise ValueError(
                        "tracks[].class deve ser string não vazia"
                    )

                entity_id = self._entity_para_rastro(
                    session_id,
                    source_id,
                    track_id,
                )

                self.atualizar_estado_memoria(
                    {
                        "session_id": session_id,
                        "source_id": source_id,
                        **track,
                    }
                )

                captura_track_ms = self._captura_ms(msg_tracks)
                self._ultima_observacao_rastro[key_rastro] = captura_track_ms

                # Em predicted não se apresenta o PPE anterior como atual.
                ppe_observado = (
                    []
                    if predicted
                    else list(
                        self._ppe_por_rastro.get(key_rastro, {}).values()
                    )
                )

                observacoes.append(
                    {
                        "session_id": session_id,
                        "source_id": source_id,
                        "track_id": track_id,
                        "track_key": key_rastro,
                        "global_id": entity_id,
                        "class": classe,
                        "confidence": float(confidence),
                        "frame_index": frame_index,
                        "timestamp_ms": captura_track_ms,
                        "point_px": point_px,
                        "position_m": position_m,
                        "space_id": space_id,
                        "zone_ids": zone_ids,
                        "ppe": ppe_observado,
                        "predicted": predicted,
                        "message_id": msg_tracks["message_id"],
                    }
                )

        clusters = self._clusterizar_observacoes(
            observacoes,
            timestamp_ms,
        )
        entidades = [
            self._montar_entidade(cluster)
            for cluster in clusters
        ]

        self._atualizar_historico_entidades(
            entidades,
            observacoes,
            timestamp_ms,
        )

        fontes = sorted({obs["source_id"] for obs in observacoes})
        fonte_principal = fontes[0] if fontes else batch[0]["source_id"]

        msg_out = self._envelope_evento(
            "ods.s2.entity",
            session_id_batch,
            timestamp_ms,
            fonte_principal,
            ARTIFACT_VERSION_FUSAO,
            message_ids[0] if message_ids else None,
        )
        msg_out["payload"] = {"entities": entidades}

        return msg_out, entidades, observacoes

    def _remover_rastro_local(
        self,
        chave: tuple[str, str, str],
    ) -> None:
        """Remove o vínculo local de um rastro encerrado por I2."""
        self._ultima_observacao_rastro.pop(chave, None)
        self._track_to_entity.pop(chave, None)
        self._ppe_por_rastro.pop(chave, None)

        chave_memoria = f"{chave[0]}:{chave[1]}:{chave[2]}"
        self.estado_em_memoria.pop(chave_memoria, None)

    # ------------------------------------------------------------------
    # Fusão e reidentificação
    # ------------------------------------------------------------------

    def _consolidar_ids_entidade(
        self,
        ids: set[str],
    ) -> str:
        """Escolhe um ID canônico e migra os estados associados."""
        if not ids:
            raise ValueError("Não é possível consolidar conjunto vazio de IDs")

        canonico = sorted(ids)[0]
        antigos = ids - {canonico}

        for antigo in antigos:
            estado_antigo = self._estado_entidades.pop(antigo, None)
            estado_canonico = self._estado_entidades.get(canonico)

            if estado_antigo and estado_canonico is None:
                self._estado_entidades[canonico] = estado_antigo

            for chave, valor in list(self._track_to_entity.items()):
                if valor == antigo:
                    self._track_to_entity[chave] = canonico

            # Migra condições ativas sem alterar seus event_id.
            for chave, estado in list(self._condicoes.items()):
                if antigo not in chave[2:]:
                    continue

                nova_chave = tuple(
                    canonico if parte == antigo else parte
                    for parte in chave
                )

                existente = self._condicoes.get(nova_chave)
                if existente is None or (
                    existente.get("event_id") is None
                    and estado.get("event_id") is not None
                ):
                    self._condicoes[nova_chave] = estado

                if nova_chave != chave:
                    self._condicoes.pop(chave, None)

            for chave, timestamp in list(
                self._ultima_observacao_entidade.items()
            ):
                if chave[1] == antigo:
                    nova_chave = (chave[0], canonico)
                    self._ultima_observacao_entidade[nova_chave] = max(
                        timestamp,
                        self._ultima_observacao_entidade.get(nova_chave, 0),
                    )
                    self._ultima_observacao_entidade.pop(chave, None)

        return canonico

    def _clusterizar_observacoes(
        self,
        observacoes: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> list[list[dict[str, Any]]]:
        clusters: list[list[dict[str, Any]]] = []

        ordenadas = sorted(
            observacoes,
            key=lambda obs: (
                obs["timestamp_ms"],
                obs["source_id"],
                obs["track_id"],
            ),
        )

        for obs in ordenadas:
            encaixe: list[dict[str, Any]] | None = None

            if obs.get("position_m") is not None and obs.get("space_id"):
                for cluster in clusters:
                    # Uma fonte não pode aparecer duas vezes no mesmo cluster.
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

                    distancia = math.hypot(x1 - x2, y1 - y2)
                    if distancia <= self.merge_distance_threshold:
                        encaixe = cluster
                        break

            if encaixe is None:
                clusters.append([obs])
            else:
                encaixe.append(obs)

        # Consolida IDs quando rastros de fontes diferentes foram fundidos.
        for cluster in clusters:
            if len(cluster) > 1:
                ids = {obs["global_id"] for obs in cluster}
                canonico = self._consolidar_ids_entidade(ids)

                for obs in cluster:
                    obs["global_id"] = canonico
                    self._track_to_entity[obs["track_key"]] = canonico

        ids_ativos = {
            obs["global_id"]
            for cluster in clusters
            for obs in cluster
        }
        ids_reutilizados: set[str] = set()

        # Reidentificação histórica: métrica no mesmo espaço ou pixel
        # somente dentro da mesma câmera.
        for cluster in clusters:
            ids_cluster = {obs["global_id"] for obs in cluster}

            if any(
                entity_id in self._estado_entidades
                for entity_id in ids_cluster
            ):
                continue

            melhor: tuple[float, str] | None = None
            session_id_cluster = cluster[0]["session_id"]

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
                    limite: float

                    if (
                        obs.get("position_m") is not None
                        and anterior.get("position_m") is not None
                        and obs.get("space_id") == anterior.get("space_id")
                        and obs.get("space_id") is not None
                    ):
                        a = obs["position_m"]
                        b = anterior["position_m"]
                        distancia = math.hypot(
                            a[0] - b[0],
                            a[1] - b[1],
                        )
                        limite = self.merge_distance_threshold

                    elif (
                        obs.get("point_px") is not None
                        and anterior.get("point_px") is not None
                        and obs.get("source_id") == anterior.get("source_id")
                        and obs.get("position_m") is None
                        and anterior.get("position_m") is None
                    ):
                        a = obs["point_px"]
                        b = anterior["point_px"]
                        distancia = math.hypot(
                            a[0] - b[0],
                            a[1] - b[1],
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

    # ------------------------------------------------------------------
    # Construção e histórico de entidades
    # ------------------------------------------------------------------

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
            obs_entidade = por_id.get(entity_id, [])
            referencia = obs_entidade[0] if obs_entidade else {}

            self._estado_entidades[entity_id] = {
                "session_id": (
                    referencia.get("session_id") if referencia else None
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
        fontes = sorted({obs["source_id"] for obs in cluster})
        zonas = sorted(
            {
                zona
                for obs in cluster
                for zona in obs.get("zone_ids", [])
            }
        )

        entidade: dict[str, Any] = {
            "entity_id": cluster[0]["global_id"],
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
                    key=lambda obs: (
                        obs["source_id"],
                        obs["track_id"],
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
        spaces = {obs["space_id"] for obs in metricas}

        if metricas and len(spaces) == 1:
            entidade["position_m"] = [
                round(
                    sum(obs["position_m"][i] for obs in metricas)
                    / len(metricas),
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
                    round(point[0]),
                    round(point[1]),
                ]

        # Não publica ponto em pixels quando a entidade contém várias fontes.
        if len(fontes) > 1 and "position_m" not in entidade:
            entidade.pop("point_px", None)

        # Consolida o último estado de EPI por item.
        ppe: dict[str, dict[str, Any]] = {}

        for obs in sorted(
            cluster,
            key=lambda item: item["timestamp_ms"],
        ):
            for item in obs.get("ppe", []):
                equipamento = item.get("equipment_item")
                if equipamento:
                    ppe[str(equipamento)] = {
                        "equipment_item": str(equipamento),
                        "compliance": item.get("compliance"),
                        "confidence": float(item.get("confidence", 0.0)),
                    }

        if ppe:
            entidade["ppe"] = list(ppe.values())

        return entidade

    # ------------------------------------------------------------------
    # Avaliação das regras
    # ------------------------------------------------------------------

    def _avaliar_regras_por_entidade(
        self,
        entidades: list[dict[str, Any]],
        observacoes: list[dict[str, Any]],
        batch: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> list[dict[str, Any]]:
        saida: list[dict[str, Any]] = []
        observacoes_por_entity: dict[
            str, list[dict[str, Any]]
        ] = defaultdict(list)

        for obs in observacoes:
            observacoes_por_entity[obs["global_id"]].append(obs)

        session_id = batch[0]["session_id"]

        for entidade in entidades:
            entity_id = entidade["entity_id"]
            obs_entity = observacoes_por_entity.get(entity_id, [])

            self._ultima_observacao_entidade[
                (session_id, entity_id)
            ] = timestamp_ms

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
                        if obs_entity
                        else None
                    ),
                    "confidence": min(
                        (obs["confidence"] for obs in obs_entity),
                        default=1.0,
                    ),
                }

                if tipo == "PPE_NON_COMPLIANT":
                    equipamento_regra = regra.get("equipment_item")
                    ppe_por_item = {
                        item.get("equipment_item"): item
                        for item in entidade.get("ppe", [])
                    }

                    equipamentos = (
                        [equipamento_regra]
                        if equipamento_regra
                        else list(ppe_por_item)
                    )

                    for equipamento in equipamentos:
                        ppe = ppe_por_item.get(equipamento)

                        # Sem avaliação atual de I9 não se infere compliance.
                        if ppe is None:
                            continue

                        compliance = ppe.get("compliance")
                        confidence = float(ppe.get("confidence", 0.0))

                        condicao = (
                            compliance in {"ABSENT", "INCORRECT"}
                            and classe_ok
                            and zona_ok
                        )

                        contexto = {
                            **context_base,
                            "equipment_item": equipamento,
                            "confidence": confidence,
                            "details": {
                                "compliance": compliance,
                                "min_duration_ms": regra["min_duration_ms"],
                            },
                        }

                        # O contrato de entidade não inclui uncertain no PPE.
                        # Para preservar a dúvida do I9 durante a avaliação,
                        # recupera-se o estado do rastro observado no quadro.
                        contexto["uncertain"] = any(
                            item.get("uncertain") is True
                            and item.get("equipment_item") == equipamento
                            for obs in obs_entity
                            for item in obs.get("ppe", [])
                        )

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
                        [zona_regra] if zona_regra else zonas
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

                    if (
                        localizacao is None
                        or regra.get("threshold") is None
                    ):
                        continue

                    unidade = (
                        "m" if entidade.get("position_m") is not None else "px"
                    )
                    chave_posicao = (session_id, entity_id)
                    atual_x = float(localizacao[0])
                    atual_y = float(localizacao[1])
                    base = self._posicao_base.get(chave_posicao)

                    if base is None or base[3] != unidade:
                        self._posicao_base[chave_posicao] = (
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
                        duracao = max(0, timestamp_ms - inicio)

                        if deslocamento > float(regra["threshold"]):
                            self._posicao_base[chave_posicao] = (
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
                                deslocamento,
                                3,
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
                            "details": {
                                "source_ids": entidade.get("source_ids", [])
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

    # ------------------------------------------------------------------
    # Persistência, debounce e emissão de eventos
    # ------------------------------------------------------------------

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
                chave,
                False,
                timestamp_ms,
                0,
            )
            return saida

        confidence = float(contexto.get("confidence", 1.0))

        if confidence < float(regra["min_confidence"]):
            return saida

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

        status = (
            "UNCERTAIN"
            if (
                contexto.get("uncertain") is True
                or confidence < float(regra["uncertain_below"])
            )
            else "CONFIRMED"
        )

        fontes = sorted(set(contexto.get("source_ids") or []))
        fonte = fontes[0] if fontes else None

        mensagem = self._envelope_evento(
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

        mensagem["payload"] = payload
        return mensagem

    # ------------------------------------------------------------------
    # Transição de zona
    # ------------------------------------------------------------------

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
        track_id = payload.get("track_id")
        classe = payload.get("class")

        if (
            transicao not in {"ENTER", "EXIT"}
            or not isinstance(zona, str)
            or not zona
            or not isinstance(track_id, str)
            or not track_id
            or not isinstance(source_id, str)
            or not source_id
            or not isinstance(classe, str)
            or not classe
        ):
            raise ValueError(
                "ods.s1.zone_transition exige source_id, track_id, class, "
                "zone_id e transition ENTER/EXIT válidos"
            )

        if transicao == "ENTER":
            entered_at = payload.get("entered_at")
            if entered_at is None:
                raise ValueError(
                    "ods.s1.zone_transition ENTER exige entered_at"
                )
            FiltroRuidoTemporal.timestamp_em_ms(entered_at)

        if transicao == "EXIT":
            dwell_s = payload.get("dwell_s")
            if (
                not isinstance(dwell_s, int)
                or isinstance(dwell_s, bool)
                or dwell_s < 0
            ):
                raise ValueError(
                    "ods.s1.zone_transition EXIT exige dwell_s integer >= 0"
                )

        chave_rastro = (session_id, source_id, track_id)

        # S1 não pode criar identidade. Só processa rastros já conhecidos
        # por I2 nesta sessão e nesta câmera.
        entity_id = self._track_to_entity.get(chave_rastro)
        if entity_id is None:
            return []

        self._ultima_observacao_rastro[chave_rastro] = max(
            timestamp_ms,
            self._ultima_observacao_rastro.get(chave_rastro, timestamp_ms),
        )

        fontes = [source_id]
        saida: list[dict[str, Any]] = []

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
                # ZONE_EXIT representa a ocorrência do cruzamento.
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

                chave_condicao = (
                    session_id,
                    regra["rule_id"],
                    entity_id,
                    zona,
                )
                estado = self._condicoes.get(chave_condicao)

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

                self._condicoes.pop(chave_condicao, None)
                self.filtro_temporal.atualizar_persistencia(
                    chave_condicao,
                    False,
                    timestamp_ms,
                    0,
                )

        return saida

    # ------------------------------------------------------------------
    # Ocupação de zona
    # ------------------------------------------------------------------

    def _processar_ocupacao(
        self,
        mensagem: dict[str, Any],
        timestamp_ms: int,
    ) -> list[dict[str, Any]]:
        payload = _payload(mensagem)
        zona = payload.get("zone_id")
        count = payload.get("count")

        if not isinstance(zona, str) or not zona.strip():
            raise ValueError(
                "ods.s1.zone_occupancy exige zone_id string não vazia"
            )

        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
        ):
            raise ValueError(
                "ods.s1.zone_occupancy exige count integer >= 0"
            )

        count_by_class = payload.get("count_by_class")
        if not isinstance(count_by_class, dict):
            raise ValueError(
                "ods.s1.zone_occupancy exige count_by_class object"
            )

        for classe, quantidade in count_by_class.items():
            if (
                not isinstance(classe, str)
                or not isinstance(quantidade, int)
                or isinstance(quantidade, bool)
                or quantidade < 0
            ):
                raise ValueError(
                    "count_by_class deve mapear classes para inteiros >= 0"
                )

        track_ids = _validar_lista_strings(
            payload.get("track_ids"),
            "track_ids",
        )

        if "max_occupancy" in payload:
            max_occupancy = payload["max_occupancy"]
            if (
                not isinstance(max_occupancy, int)
                or isinstance(max_occupancy, bool)
                or max_occupancy < 0
            ):
                raise ValueError(
                    "max_occupancy deve ser integer >= 0"
                )

        session_id = mensagem["session_id"]
        fonte = mensagem.get("source_id")
        chave_zona = (session_id, zona)
        anterior = self._contagem_zonas.get(chave_zona)
        self._contagem_zonas[chave_zona] = count

        if anterior is None:
            return []

        saida: list[dict[str, Any]] = []

        for regra in self.regras:
            tipo = regra["event_type"]
            threshold = regra.get("threshold")

            if (
                tipo not in {"OCCUPANCY_ABOVE", "OCCUPANCY_BELOW"}
                or threshold is None
            ):
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
                "details": {
                    "count": count,
                    "previous_count": anterior,
                    "track_ids": track_ids,
                },
            }

            chave = (zona,)

            if cruzou:
                saida.extend(
                    self._atualizar_condicao(
                        regra,
                        chave,
                        True,
                        timestamp_ms,
                        contexto,
                        session_id,
                        persistencia_override_ms=0,
                    )
                )

                chave_condicao = (
                    session_id,
                    regra["rule_id"],
                    *chave,
                )
                estado = self._condicoes.get(chave_condicao)

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

                self._condicoes.pop(chave_condicao, None)
                self.filtro_temporal.atualizar_persistencia(
                    chave_condicao,
                    False,
                    timestamp_ms,
                    0,
                )

        return saida

    # ------------------------------------------------------------------
    # Encerramento por ausência e expiração
    # ------------------------------------------------------------------

    def _encerrar_condicoes_de_entidades_ausentes(
        self,
        entidades: list[dict[str, Any]],
        timestamp_ms: int,
        session_id: str,
        saida: list[dict[str, Any]],
    ) -> None:
        presentes = {entidade["entity_id"] for entidade in entidades}

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
                    regra
                    for regra in self.regras
                    if regra["rule_id"] == chave[1]
                ),
                None,
            )
            if regra is None:
                continue

            ultimo = int(estado.get("last_seen_ms", timestamp_ms))
            if timestamp_ms - ultimo <= self.ttl_threshold_ms:
                continue

            contexto = dict(estado.get("contexto", {}))

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
                chave,
                False,
                timestamp_ms,
                0,
            )

    def _expirar_rastros(
        self,
        session_id: str,
        timestamp_ms: int,
    ) -> None:
        """Expira rastros com base em captured_at, não no relógio local."""
        expirados = [
            chave
            for chave, ultimo in self._ultima_observacao_rastro.items()
            if (
                chave[0] == session_id
                and timestamp_ms - ultimo > self.ttl_threshold_ms
            )
        ]

        for chave in expirados:
            self._remover_rastro_local(chave)

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

    # ------------------------------------------------------------------
    # Reinicialização de sessão
    # ------------------------------------------------------------------

    def _resetar_estado_sessao(self, session_id: str) -> None:
        self._estado_entidades = {
            chave: valor
            for chave, valor in self._estado_entidades.items()
            if valor.get("session_id") != session_id
        }

        self.estado_em_memoria = {
            chave: valor
            for chave, valor in self.estado_em_memoria.items()
            if not chave.startswith(f"{session_id}:")
        }

        self._track_to_entity = {
            chave: valor
            for chave, valor in self._track_to_entity.items()
            if chave[0] != session_id
        }

        self._ppe_por_rastro = {
            chave: valor
            for chave, valor in self._ppe_por_rastro.items()
            if chave[0] != session_id
        }

        self._ultima_observacao_rastro = {
            chave: valor
            for chave, valor in self._ultima_observacao_rastro.items()
            if chave[0] != session_id
        }

        self._condicoes = {
            chave: valor
            for chave, valor in self._condicoes.items()
            if chave[0] != session_id
        }

        self._contagem_zonas = {
            chave: valor
            for chave, valor in self._contagem_zonas.items()
            if chave[0] != session_id
        }

        self._inicio_zona = {
            chave: valor
            for chave, valor in self._inicio_zona.items()
            if chave[0] != session_id
        }

        self._posicao_base = {
            chave: valor
            for chave, valor in self._posicao_base.items()
            if chave[0] != session_id
        }

        self._ultima_observacao_entidade = {
            chave: valor
            for chave, valor in self._ultima_observacao_entidade.items()
            if chave[0] != session_id
        }

        self._sequence_por_sessao[session_id] = 0
        self.filtro_temporal.limpar_sessao(session_id)
