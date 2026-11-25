
from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timezone

from components.S2.filtro_ruido_temporal import FiltroRuidoTemporal
from components.S2.motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente

SESSION = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"
BASE = 1_800_000_000_000


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def envelope(schema: str, source: str, frame: int, ts: int, payload: dict, producer: str) -> dict:
    return {
        "message_type": "event",
        "message_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{schema}:{source}:{frame}")),
        "schema": schema,
        "schema_version": "1.0",
        "producer": producer,
        "published_at": iso(ts),
        "captured_at": iso(ts),
        "source_id": source,
        "session_id": SESSION,
        "sequence": frame,
        "artifact_version": "fixture-1",
        "payload": payload,
    }


def frame_messages(frame: int, ts: int, sources=("cam0", "cam1"), confidence: float = 0.9):
    messages = []
    for index, source in enumerate(sources):
        track_id = f"track-{source}"
        track = {
            "track_id": track_id,
            "detection_id": 0,
            "class": "person",
            "state": "CONFIRMED",
            "predicted": False,
            "confidence": confidence,
            "point_px": [100, 200],
            "bbox": [90, 100, 110, 200],
        }
        pos = {
            "track_id": track_id,
            "class": "person",
            "point_px": [100, 200],
            "position_m": [1.0 + index * 0.05, 2.0],
            "space_id": "world-1",
            "zone_ids": ["zona-demo"],
        }
        messages.append(envelope("ods.i2.tracks", source, frame, ts, {"frame_index": frame, "tracks": [track]}, "I2"))
        messages.append(envelope("ods.s1.position", source, frame, ts, {"frame_index": frame, "positions": [pos]}, "S1"))
    return messages


def rules(min_duration_ms=1000, min_confidence=0.5, uncertain_below=0.8, debounce_ms=500):
    return {
        "artifact_version": "rules-test-1",
        "rules": [{
            "rule_id": "cross-camera-test",
            "event_type": "CROSS_CAMERA_MATCH",
            "min_confidence": min_confidence,
            "uncertain_below": uncertain_below,
            "min_duration_ms": min_duration_ms,
            "debounce_ms": debounce_ms,
        }],
    }


def rules_command(
    message_id: str | None = None,
    artifact_version: str = "rules-test-2",
    ts: int = BASE,
    rule_list: list | None = None,
) -> dict:
    configuracao = rules(min_duration_ms=0, debounce_ms=0)

    return {
        "message_type": "command",
        "message_id": message_id or str(uuid.uuid4()),
        "schema": "ods.s2.rules.command",
        "schema_version": "1.0",
        "producer": "A3",
        "published_at": iso(ts),
        "target": "S2",
        "payload": {
            "artifact_version": artifact_version,
            "rules": (
                configuracao["rules"]
                if rule_list is None
                else rule_list
            ),
        },
    }

class TestFiltroTemporal(unittest.TestCase):
    def test_persistencia_so_confirma_apos_tempo_minimo(self):
        filtro = FiltroRuidoTemporal()
        self.assertEqual(filtro.atualizar_persistencia("k", True, 1000, 5000), (False, 0))
        self.assertEqual(filtro.atualizar_persistencia("k", True, 5999, 5000), (False, 4999))
        self.assertEqual(filtro.atualizar_persistencia("k", True, 6000, 5000), (True, 5000))

    def test_debounce_nao_descarta_observacoes_de_estado(self):
        filtro = FiltroRuidoTemporal(500)
        self.assertTrue(filtro.aplicar_debounce("event", 1000))
        self.assertFalse(filtro.aplicar_debounce("event", 1200))
        self.assertTrue(filtro.aplicar_debounce("event", 1500))


class TestMotorS2(unittest.TestCase):
    def test_session_started_tem_prioridade_sobre_quadro_com_mesmo_timestamp(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        # Simula um vínculo antigo que precisa ser removido no início da sessão.
        motor._track_to_entity[(SESSION, "cam0", "track-cam0")] = "entity-antiga"

        inicio_sessao = {
            "message_type": "event",
            "message_id": str(uuid.uuid4()),
            "schema": "ods.session.started",
            "schema_version": "1.0",
            "producer": "B1",
            "published_at": iso(BASE),
            "session_id": SESSION,
            "sequence": 0,
            "payload": {},
        }

        quadros = frame_messages(1, BASE, sources=("cam0",))

        saida = motor.processar_mensagens([inicio_sessao] + quadros)

        mensagem_entidade = next(
            m for m in saida if m["schema"] == "ods.s2.entity"
        )
        ids_entidades = {
            entidade["entity_id"]
            for entidade in mensagem_entidade["payload"]["entities"]
        }

        self.assertNotIn("entity-antiga", ids_entidades)

    def test_persistencia_minima_e_emissao_started_updated(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        motor.carregar_regras(rules(min_duration_ms=1000, debounce_ms=500))
        entrada = frame_messages(1, BASE) + frame_messages(2, BASE + 1000) + frame_messages(3, BASE + 1600)
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertEqual([e["phase"] for e in eventos], ["STARTED", "UPDATED"])
        self.assertEqual(eventos[0]["duration_ms"], 1000)
        self.assertEqual(eventos[0]["status"], "CONFIRMED")

    def test_resetar_sessao_preserva_estado_das_demais_sessoes(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        outra_sessao = str(uuid.uuid4())

        motor._track_to_entity[(SESSION, "cam0", "track-1")] = "entity-1"
        motor._track_to_entity[(outra_sessao, "cam1", "track-2")] = "entity-2"

        motor._sequence_por_sessao[SESSION] = 15
        motor._sequence_por_sessao[outra_sessao] = 8

        motor._resetar_estado_sessao(SESSION)

        self.assertNotIn(
            (SESSION, "cam0", "track-1"),
            motor._track_to_entity,
        )
        self.assertEqual(
            motor._track_to_entity[(outra_sessao, "cam1", "track-2")],
            "entity-2",
        )

        self.assertEqual(motor._sequence_por_sessao[SESSION], 0)
        self.assertEqual(motor._sequence_por_sessao[outra_sessao], 8)

    def test_sequence_recomeca_em_zero_apos_session_started(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        motor._sequence_por_sessao[SESSION] = 15

        inicio_sessao = {
            "message_type": "event",
            "message_id": str(uuid.uuid4()),
            "schema": "ods.session.started",
            "schema_version": "1.0",
            "producer": "B1",
            "published_at": iso(BASE),
            "session_id": SESSION,
            "sequence": 0,
            "payload": {},
        }

        saida = motor.processar_mensagens(
            [inicio_sessao] + frame_messages(1, BASE, sources=("cam0",))
        )

        mensagem_entidade = next(
            m for m in saida if m["schema"] == "ods.s2.entity"
        )

        self.assertEqual(mensagem_entidade["sequence"], 0)

    def test_remocao_de_regra_encerra_evento_ativo(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0, debounce_ms=0))

        saida_inicial = motor.processar_mensagens(frame_messages(1, BASE))
        eventos_iniciais = [
            m for m in saida_inicial if m["schema"] == "ods.s2.event"
        ]
        self.assertEqual(eventos_iniciais[0]["payload"]["phase"], "STARTED")

        eventos_encerrados = motor.carregar_regras({
            "artifact_version": "rules-test-2",
            "rules": [],
        })

        self.assertEqual(len(eventos_encerrados), 1)
        self.assertEqual(eventos_encerrados[0]["payload"]["phase"], "ENDED")
        self.assertEqual(
            eventos_encerrados[0]["payload"]["event_id"],
            eventos_iniciais[0]["payload"]["event_id"],
        )
        self.assertFalse(motor._condicoes)

    def test_consolidacao_preserva_inicio_mais_antigo_da_persistencia(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        chave_a = (SESSION, "regra-1", "e-000001")
        chave_b = (SESSION, "regra-1", "e-000002")

        motor.filtro_temporal.inicio_condicao[chave_a] = BASE
        motor.filtro_temporal.inicio_condicao[chave_b] = BASE + 500

        motor._consolidar_ids_entidade({"e-000001", "e-000002"})

        chave_consolidada = (SESSION, "regra-1", "e-000001")

        self.assertEqual(
            motor.filtro_temporal.inicio_condicao[chave_consolidada],
            BASE,
        )

        persistiu, duracao = motor.filtro_temporal.atualizar_persistencia(
            chave_consolidada,
            True,
            BASE + 1000,
            1000,
        )

        self.assertTrue(persistiu)
        self.assertEqual(duracao, 1000)

    def test_consolidar_entidades_preserva_eventos_ativos_distintos(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        event_id_1 = str(uuid.uuid4())
        event_id_2 = str(uuid.uuid4())

        chave_1 = (SESSION, "cross-camera-test", "e-000001")
        chave_2 = (SESSION, "cross-camera-test", "e-000002")

        contexto_1 = {
            "entity_id": "e-000001",
            "source_ids": ["cam0", "cam1"],
            "confidence": 0.9,
        }
        contexto_2 = {
            "entity_id": "e-000002",
            "source_ids": ["cam2", "cam3"],
            "confidence": 0.8,
        }

        motor._condicoes[chave_1] = {
            "first_seen_ms": BASE,
            "last_seen_ms": BASE + 1000,
            "last_emitted_ms": BASE + 1000,
            "event_id": event_id_1,
            "contexto": contexto_1,
        }
        motor._condicoes[chave_2] = {
            "first_seen_ms": BASE + 500,
            "last_seen_ms": BASE + 1000,
            "last_emitted_ms": BASE + 1000,
            "event_id": event_id_2,
            "contexto": contexto_2,
        }

        canonico = motor._consolidar_ids_entidade(
            {"e-000001", "e-000002"}
        )

        eventos_ativos = [
            item["event_id"]
            for chave, estado in motor._condicoes.items()
            if chave[1] == "cross-camera-test"
            for item in motor._estados_da_condicao(estado)
            if item.get("event_id") is not None
        ]

        self.assertEqual(canonico, "e-000001")
        self.assertCountEqual(
            eventos_ativos,
            [event_id_1, event_id_2],
        )



    def test_consolidacoes_sucessivas_preservam_todos_os_eventos(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        event_ids = [str(uuid.uuid4()) for _ in range(3)]

        for i, event_id in enumerate(event_ids, start=1):
            entity_id = f"e-{i:06d}"
            chave = (SESSION, "cross-camera-test", entity_id)

            motor._condicoes[chave] = {
                "first_seen_ms": BASE + i * 100,
                "last_seen_ms": BASE + 1000,
                "last_emitted_ms": BASE + 1000,
                "event_id": event_id,
                "contexto": {"entity_id": entity_id},
            }

        motor._consolidar_ids_entidade(
            {"e-000001", "e-000002"}
        )
        motor._consolidar_ids_entidade(
            {"e-000001", "e-000003"}
        )

        eventos_ativos = [
            item["event_id"]
            for chave, estado in motor._condicoes.items()
            if chave[1] == "cross-camera-test"
            for item in motor._estados_da_condicao(estado)
            if item.get("event_id") is not None
        ]

        self.assertCountEqual(eventos_ativos, event_ids)
        self.assertEqual(len(motor._condicoes), 1)


    def test_consolidacao_atualiza_todos_os_eventos_ativos(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        regra = rules(min_duration_ms=0, debounce_ms=500)["rules"][0]

        event_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        for i, event_id in enumerate(event_ids, start=1):
            entity_id = f"e-{i:06d}"
            chave = (SESSION, regra["rule_id"], entity_id)

            motor._condicoes[chave] = {
                "first_seen_ms": BASE,
                "last_seen_ms": BASE + 1000,
                "last_emitted_ms": BASE + 1000,
                "event_id": event_id,
                "contexto": {"entity_id": entity_id},
            }

        motor._consolidar_ids_entidade(
            {"e-000001", "e-000002"}
        )

        eventos = motor._atualizar_condicao(
            regra,
            ("e-000001",),
            True,
            BASE + 2000,
            {"confidence": 0.95},
            SESSION,
        )

        self.assertEqual(len(eventos), 2)
        self.assertEqual(
            {evento["payload"]["phase"] for evento in eventos},
            {"UPDATED"},
        )
        self.assertCountEqual(
            [
                evento["payload"]["event_id"]
                for evento in eventos
            ],
            event_ids,
        )

    def test_ttl_encerra_individualmente_eventos_apos_consolidacao(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        regra = rules(min_duration_ms=0, debounce_ms=0)["rules"][0]
        motor.carregar_regras(rules(min_duration_ms=0, debounce_ms=0))

        event_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        for i, event_id in enumerate(event_ids, start=1):
            entity_id = f"e-{i:06d}"
            chave = (SESSION, regra["rule_id"], entity_id)

            motor._condicoes[chave] = {
                "first_seen_ms": BASE,
                "last_seen_ms": BASE if i == 1 else BASE + 8000,
                "last_emitted_ms": BASE,
                "event_id": event_id,
                "contexto": {"entity_id": entity_id},
            }

        motor._consolidar_ids_entidade(
            {"e-000001", "e-000002"}
        )

        saida = []
        motor._encerrar_condicoes_de_entidades_ausentes(
            [],
            BASE + 10000,
            SESSION,
            saida,
        )

        self.assertEqual(len(saida), 1)
        self.assertEqual(saida[0]["payload"]["phase"], "ENDED")
        self.assertEqual(saida[0]["payload"]["event_id"], event_ids[0])

        estados_restantes = [
            item
            for estado in motor._condicoes.values()
            for item in motor._estados_da_condicao(estado)
        ]

        self.assertEqual(len(estados_restantes), 1)
        self.assertEqual(estados_restantes[0]["event_id"], event_ids[1])

    def test_consolidacao_preserva_eventos_ao_remover_regra(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        regra = rules(min_duration_ms=0, debounce_ms=0)["rules"][0]
        motor.regras = [regra]

        event_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        for i, event_id in enumerate(event_ids):
            chave = (SESSION, regra["rule_id"], f"e-{i + 1:06d}")
            motor._condicoes[chave] = {
                "first_seen_ms": BASE + i * 100,
                "last_seen_ms": BASE + 1000,
                "last_emitted_ms": BASE + 1000,
                "event_id": event_id,
                "contexto": {"entity_id": f"e-{i + 1:06d}"},
            }

        motor._consolidar_ids_entidade({"e-000001", "e-000002"})

        eventos_encerrados = motor.carregar_regras({
            "artifact_version": "rules-test-2",
            "rules": [],
        })

        self.assertEqual(len(eventos_encerrados), 2)
        self.assertEqual(
            {evento["payload"]["phase"] for evento in eventos_encerrados},
            {"ENDED"},
        )
        self.assertCountEqual(
            [
                evento["payload"]["event_id"]
                for evento in eventos_encerrados
            ],
            event_ids,
        )
        self.assertFalse(motor._condicoes)


    def test_comando_regras_valido_gera_reply(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        comando = rules_command()

        saida = motor.processar_mensagens([comando])
        replies = [
            m for m in saida
            if m["schema"] == "ods.s2.rules.reply"
        ]

        self.assertEqual(len(replies), 1)

        reply = replies[0]
        self.assertEqual(reply["message_type"], "reply")
        self.assertEqual(
            reply["correlation_id"],
            comando["message_id"],
        )
        self.assertEqual(reply["payload"]["status"], "OK")
        self.assertEqual(
            reply["payload"]["applied_rule_ids"],
            ["cross-camera-test"],
        )
        self.assertNotIn("session_id", reply)
        self.assertNotIn("sequence", reply)
        self.assertNotIn("captured_at", reply)

    def test_comando_repetido_reutiliza_reply_original(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        comando = rules_command()

        primeira_saida = motor.processar_mensagens([comando])
        primeira_reply = next(
            m for m in primeira_saida
            if m["schema"] == "ods.s2.rules.reply"
        )

        segunda_saida = motor.processar_mensagens([comando])
        segunda_reply = next(
            m for m in segunda_saida
            if m["schema"] == "ods.s2.rules.reply"
        )

        self.assertEqual(segunda_reply, primeira_reply)
        self.assertEqual(
            motor.artifact_version_regras,
            "rules-test-2",
        )

    def test_comando_duplicado_no_mesmo_lote_responde_duas_vezes(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        comando = rules_command()

        saida = motor.processar_mensagens([
            comando,
            dict(comando),
        ])

        replies = [
            m for m in saida
            if m["schema"] == "ods.s2.rules.reply"
        ]

        self.assertEqual(len(replies), 2)
        self.assertEqual(replies[0], replies[1])

    def test_comando_antigo_repetido_nao_reaplica_regras(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        comando_antigo = rules_command(
            artifact_version="rules-test-2",
            ts=BASE,
        )
        comando_novo = rules_command(
            artifact_version="rules-test-3",
            ts=BASE + 1000,
        )

        motor.processar_mensagens([comando_antigo])
        motor.processar_mensagens([comando_novo])

        saida_repeticao = motor.processar_mensagens([
            comando_antigo
        ])

        replies = [
            m for m in saida_repeticao
            if m["schema"] == "ods.s2.rules.reply"
        ]

        self.assertEqual(len(replies), 1)
        self.assertEqual(
            replies[0]["correlation_id"],
            comando_antigo["message_id"],
        )
        self.assertEqual(
            motor.artifact_version_regras,
            "rules-test-3",
        )

    def test_comando_com_event_type_desconhecido_retorna_erro(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        comando = rules_command(
            rule_list=[{
                "rule_id": "regra-invalida",
                "event_type": "EVENTO_INEXISTENTE",
                "min_confidence": 0.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }]
        )

        saida = motor.processar_mensagens([comando])

        reply = next(
            m for m in saida
            if m["schema"] == "ods.s2.rules.reply"
        )

        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(
            reply["payload"]["error"]["code"],
            "UNKNOWN_EVENT_TYPE",
        )
        self.assertIn(
            "EVENTO_INEXISTENTE",
            reply["payload"]["error"]["message"],
        )
        self.assertEqual(
            reply["correlation_id"],
            comando["message_id"],
        )
        self.assertNotIn("code", reply["payload"])
        self.assertNotIn("message", reply["payload"])

    def test_comando_com_regra_invalida_preserva_regras_anteriores(self):
        motor = MotorGerenciamentoEstadoConcorrente()

        comando_valido = rules_command(
            artifact_version="rules-validas"
        )
        motor.processar_mensagens([comando_valido])

        regras_anteriores = [dict(r) for r in motor.regras]

        comando_invalido = rules_command(
            artifact_version="rules-invalidas",
            rule_list=[{
                "rule_id": "regra-invalida",
                "event_type": "CROSS_CAMERA_MATCH",
                "min_confidence": 1.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }]
        )

        saida = motor.processar_mensagens([comando_invalido])

        reply = next(
            m for m in saida
            if m["schema"] == "ods.s2.rules.reply"
        )

        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(
            reply["payload"]["error"]["code"],
            "INVALID_RULE",
        )
        self.assertEqual(
            reply["correlation_id"],
            comando_invalido["message_id"],
        )
        self.assertEqual(motor.artifact_version_regras, "rules-validas")
        self.assertEqual(motor.regras, regras_anteriores)

    def test_modificacao_de_regra_encerra_evento_ativo(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0, debounce_ms=0))

        saida_inicial = motor.processar_mensagens(frame_messages(1, BASE))
        evento_inicial = next(
            m for m in saida_inicial if m["schema"] == "ods.s2.event"
        )

        novas_regras = rules(min_duration_ms=0, debounce_ms=0)
        novas_regras["artifact_version"] = "rules-test-2"
        novas_regras["rules"][0]["min_confidence"] = 0.7

        eventos_encerrados = motor.carregar_regras(novas_regras)

        self.assertEqual(len(eventos_encerrados), 1)
        self.assertEqual(eventos_encerrados[0]["payload"]["phase"], "ENDED")
        self.assertEqual(
            eventos_encerrados[0]["payload"]["event_id"],
            evento_inicial["payload"]["event_id"],
        )
        self.assertEqual(
            eventos_encerrados[0]["artifact_version"],
            "rules-test-1",
        )

    def test_regra_inalterada_preserva_evento_ativo(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        configuracao = rules(min_duration_ms=0, debounce_ms=0)
        motor.carregar_regras(configuracao)
        motor.processar_mensagens(frame_messages(1, BASE))

        eventos_encerrados = motor.carregar_regras({
            "artifact_version": "rules-test-2",
            "rules": [dict(configuracao["rules"][0])],
        })

        self.assertEqual(eventos_encerrados, [])
        self.assertTrue(motor._condicoes)
        self.assertEqual(motor.artifact_version_regras, "rules-test-2")

    def test_artefato_invalido_preserva_regras_e_condicoes_ativas(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        configuracao = rules(min_duration_ms=0, debounce_ms=0)
        motor.carregar_regras(configuracao)
        motor.processar_mensagens(frame_messages(1, BASE))

        regras_anteriores = [dict(regra) for regra in motor.regras]
        condicoes_anteriores = dict(motor._condicoes)

        with self.assertRaises(ValueError):
            motor.carregar_regras({
                "artifact_version": "rules-invalid",
                "rules": "isso-nao-e-uma-lista",
            })

        self.assertEqual(motor.regras, regras_anteriores)
        self.assertEqual(motor._condicoes, condicoes_anteriores)
        self.assertEqual(motor.artifact_version_regras, "rules-test-1")

    def test_confianca_intermediaria_publica_uncertain(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0))
        entrada = frame_messages(1, BASE, confidence=0.6)
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertTrue(eventos)
        self.assertEqual(eventos[0]["status"], "UNCERTAIN")


    def test_queda_de_confianca_encerra_evento_cross_camera_ativo(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(
            rules(
                min_duration_ms=0,
                min_confidence=0.5,
                debounce_ms=0,
            )
        )

        saida_inicial = motor.processar_mensagens(
            frame_messages(1, BASE, confidence=0.9)
        )
        eventos_iniciais = [
            m for m in saida_inicial
            if m["schema"] == "ods.s2.event"
        ]

        self.assertEqual(
            [m["payload"]["phase"] for m in eventos_iniciais],
            ["STARTED"],
        )

        event_id = eventos_iniciais[0]["payload"]["event_id"]

        saida_baixa = motor.processar_mensagens(
            frame_messages(2, BASE + 1000, confidence=0.4)
        )
        eventos_baixa = [
            m for m in saida_baixa
            if m["schema"] == "ods.s2.event"
        ]

        self.assertEqual(
            [m["payload"]["phase"] for m in eventos_baixa],
            ["ENDED"],
        )
        self.assertEqual(
            eventos_baixa[0]["payload"]["event_id"],
            event_id,
        )
        self.assertFalse(motor._condicoes)


    def test_confianca_abaixo_do_minimo_e_ignorada(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0, min_confidence=0.5))
        saida = motor.processar_mensagens(frame_messages(1, BASE, confidence=0.4))
        self.assertFalse([m for m in saida if m["schema"] == "ods.s2.event"])

    def test_ttl_preserva_id_dentro_da_janela_e_renova_apos_expirar(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        uma_camera = lambda frame, ts: frame_messages(frame, ts, sources=("cam0",))
        saida = motor.processar_mensagens(
            uma_camera(1, BASE) + uma_camera(2, BASE + 4000) + uma_camera(3, BASE + 10001)
        )
        entidades = [e for m in saida if m["schema"] == "ods.s2.entity" for e in m["payload"]["entities"]]
        ids = [e["entity_id"] for e in entidades]
        self.assertEqual(ids[0], ids[1])
        self.assertNotEqual(ids[1], ids[2])

    def test_match_multicamera_nao_encerra_na_primeira_oclusao(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        motor.carregar_regras(rules(min_duration_ms=0, debounce_ms=0))
        entrada = frame_messages(1, BASE) + frame_messages(2, BASE + 1000, sources=("cam0",))
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertEqual([e["phase"] for e in eventos], ["STARTED"])

    def test_ppe_catalog_rejeita_identificadores_com_espacos(self):
        catalogo = {
            "catalog_version": "ppe-catalog-whitespace-1",
            "items": [
                {
                    "equipment_item": " luva ",
                    "name": "Luvas",
                    "body_part": "HANDS",
                }
            ],
            "required": [
                {
                    "zone_id": "galpao-solda",
                    "items": [" luva "],
                }
            ],
        }

        with self.assertRaises(ValueError):
            MotorGerenciamentoEstadoConcorrente(
                artefato_ppe_catalog=catalogo
            )

        catalogo["items"][0]["equipment_item"] = "luva"
        catalogo["required"][0]["items"] = ["luva"]
        catalogo["required"][0]["zone_id"] = " galpao-solda "

        with self.assertRaises(ValueError):
            MotorGerenciamentoEstadoConcorrente(
                artefato_ppe_catalog=catalogo
            )

    def test_evento_ppe_encerra_apos_ttl_sem_observacao(self):
        motor = MotorGerenciamentoEstadoConcorrente(
            ttl_threshold_ms=5000
        )
        motor.carregar_regras({
            "artifact_version": "rules-ppe-ttl-1",
            "rules": [{
                "rule_id": "ppe-ttl-test",
                "event_type": "PPE_NON_COMPLIANT",
                "equipment_item": "luva",
                "min_confidence": 0.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }],
        })

        entrada = frame_messages(1, BASE, sources=("cam0",))

        entrada.append(envelope(
            "ods.i9.ppe_compliance",
            "cam0",
            1,
            BASE,
            {
                "frame_index": 1,
                "bbox": [90, 100, 110, 200],
                "calibrated": False,
                "catalog_version": "ppe-catalog-1",
                "detection_id": 0,
                "uncertain": False,
                "items": [{
                    "equipment_item": "luva",
                    "compliance": "ABSENT",
                    "confidence": 0.95,
                }],
            },
            "I9",
        ))

        entrada.extend([
            envelope(
                "ods.i2.tracks",
                "cam0",
                2,
                BASE + 6001,
                {"frame_index": 2, "tracks": []},
                "I2",
            ),
            envelope(
                "ods.s1.position",
                "cam0",
                2,
                BASE + 6001,
                {"frame_index": 2, "positions": []},
                "S1",
            ),
        ])

        saida = motor.processar_mensagens(entrada)

        eventos = [
            mensagem["payload"]
            for mensagem in saida
            if mensagem["schema"] == "ods.s2.event"
        ]

        self.assertEqual(
            [evento["phase"] for evento in eventos],
            ["STARTED", "ENDED"],
        )
        self.assertEqual(
            eventos[0]["event_id"],
            eventos[1]["event_id"],
        )
        self.assertEqual(eventos[1]["equipment_item"], "luva")
        self.assertEqual(eventos[1]["zone_id"], "zona-demo")

    def test_ppe_separa_condicoes_por_zona_e_equipamento(self):
        catalogo = {
            "catalog_version": "ppe-catalog-multizone-1",
            "items": [
                {"equipment_item": "luva", "name": "Luvas", "body_part": "HANDS"},
                {"equipment_item": "capacete", "name": "Capacete", "body_part": "HEAD"},
            ],
            "required": [
                {"zone_id": "galpao-solda", "items": ["luva"]},
                {"zone_id": "area-corte", "items": ["capacete"]},
            ],
        }

        motor = MotorGerenciamentoEstadoConcorrente(
            artefato_ppe_catalog=catalogo
        )
        motor.carregar_regras({
            "artifact_version": "rules-ppe-multizone-1",
            "rules": [{
                "rule_id": "ppe-multizone-test",
                "event_type": "PPE_NON_COMPLIANT",
                "min_confidence": 0.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }],
        })

        entrada = frame_messages(1, BASE, sources=("cam0",))

        for mensagem in entrada:
            if mensagem["schema"] == "ods.s1.position":
                mensagem["payload"]["positions"][0]["zone_ids"] = [
                    "galpao-solda",
                    "area-corte",
                ]

        entrada.append(envelope(
            "ods.i9.ppe_compliance",
            "cam0",
            1,
            BASE,
            {
                "frame_index": 1,
                "bbox": [90, 100, 110, 200],
                "calibrated": False,
                "catalog_version": "ppe-catalog-1",
                "detection_id": 0,
                "uncertain": False,
                "items": [
                    {
                        "equipment_item": "luva",
                        "compliance": "ABSENT",
                        "confidence": 0.95,
                    },
                    {
                        "equipment_item": "capacete",
                        "compliance": "INCORRECT",
                        "confidence": 0.90,
                    },
                ],
            },
            "I9",
        ))

        saida = motor.processar_mensagens(entrada)
        eventos = [
            mensagem["payload"]
            for mensagem in saida
            if mensagem["schema"] == "ods.s2.event"
        ]

        combinacoes = {
            (evento["zone_id"], evento["equipment_item"])
            for evento in eventos
        }

        self.assertEqual(len(eventos), 2)
        self.assertEqual(
            combinacoes,
            {
                ("galpao-solda", "luva"),
                ("area-corte", "capacete"),
            },
        )
        self.assertTrue(
            all(evento["phase"] == "STARTED" for evento in eventos)
        )

    def test_correlaciona_i9_com_i2_por_detection_id_e_quadro(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras({
            "artifact_version": "rules-ppe-test-1",
            "rules": [{
                "rule_id": "ppe-glove-test", "event_type": "PPE_NON_COMPLIANT",
                "equipment_item": "luva", "min_confidence": 0.5,
                "uncertain_below": 0.8, "min_duration_ms": 1000, "debounce_ms": 0,
            }],
        })
        entrada = []
        for frame, ts in ((1, BASE), (2, BASE + 1000)):
            entrada.extend(frame_messages(frame, ts, sources=("cam0",)))
            entrada.append(envelope(
                "ods.i9.ppe_compliance", "cam0", frame, ts,
                {
                    "frame_index": frame,
                    "bbox": [90, 100, 110, 200],
                    "calibrated": False,
                    "catalog_version": "ppe-catalog-1",
                    "detection_id": 0,
                    "uncertain": False,
                    "items": [
                        {
                            "equipment_item": "luva",
                            "compliance": "ABSENT",
                            "confidence": 0.7
                        }
                    ]
                }, "I9",
            ))
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertTrue(eventos)
        self.assertEqual(eventos[0]["event_type"], "PPE_NON_COMPLIANT")
        self.assertEqual(eventos[0]["equipment_item"], "luva")
        self.assertEqual(eventos[0]["status"], "UNCERTAIN")

    def test_ppe_catalog_avalia_somente_itens_observados_e_obrigatorios(self):
        catalogo = {
            "catalog_version": "ppe-catalog-test-1",
            "items": [
                {"equipment_item": "capacete", "name": "Capacete", "body_part": "HEAD"},
                {"equipment_item": "luva", "name": "Luvas", "body_part": "HANDS"},
                {"equipment_item": "oculos", "name": "Óculos", "body_part": "EYES"},
            ],
            "required": [
                {
                    "zone_id": "galpao-solda",
                    "items": ["capacete", "luva", "oculos"],
                }
            ],
        }

        motor = MotorGerenciamentoEstadoConcorrente(
            artefato_ppe_catalog=catalogo
        )
        motor.carregar_regras({
            "artifact_version": "rules-ppe-catalog-test-1",
            "rules": [{
                "rule_id": "ppe-zone-test",
                "event_type": "PPE_NON_COMPLIANT",
                "min_confidence": 0.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }],
        })

        entrada = frame_messages(1, BASE, sources=("cam0",))

        for mensagem in entrada:
            if mensagem["schema"] == "ods.s1.position":
                mensagem["payload"]["positions"][0]["zone_ids"] = [
                    "galpao-solda"
                ]

        entrada.append(envelope(
            "ods.i9.ppe_compliance",
            "cam0",
            1,
            BASE,
            {
                "frame_index": 1,
                "bbox": [90, 100, 110, 200],
                "calibrated": False,
                "catalog_version": "ppe-catalog-1",
                "detection_id": 0,
                "uncertain": False,
                "items": [{
                    "equipment_item": "luva",
                    "compliance": "ABSENT",
                    "confidence": 0.95,
                }],
            },
            "I9",
        ))

        saida = motor.processar_mensagens(entrada)
        eventos = [
            mensagem["payload"]
            for mensagem in saida
            if mensagem["schema"] == "ods.s2.event"
        ]

        self.assertEqual(len(eventos), 1)
        self.assertEqual(eventos[0]["event_type"], "PPE_NON_COMPLIANT")
        self.assertEqual(eventos[0]["equipment_item"], "luva")
        self.assertEqual(eventos[0]["zone_id"], "galpao-solda")
        self.assertEqual(eventos[0]["status"], "CONFIRMED")

    def test_ppe_nao_reutiliza_avaliacao_de_quadro_anterior(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras({
            "artifact_version": "rules-ppe-freshness-test-1",
            "rules": [{
                "rule_id": "ppe-freshness-test",
                "event_type": "PPE_NON_COMPLIANT",
                "equipment_item": "luva",
                "min_confidence": 0.5,
                "uncertain_below": 0.8,
                "min_duration_ms": 0,
                "debounce_ms": 0,
            }],
        })

        entrada = []

        for frame, ts in ((1, BASE), (2, BASE + 1000)):
            entrada.extend(
                frame_messages(frame, ts, sources=("cam0",))
            )

            if frame == 1:
                entrada.append(envelope(
                    "ods.i9.ppe_compliance",
                    "cam0",
                    frame,
                    ts,
                    {
                        "frame_index": frame,
                        "bbox": [90, 100, 110, 200],
                        "calibrated": False,
                        "catalog_version": "ppe-catalog-1",
                        "detection_id": 0,
                        "uncertain": False,
                        "items": [{
                            "equipment_item": "luva",
                            "compliance": "ABSENT",
                            "confidence": 0.95,
                        }],
                    },
                    "I9",
                ))

        saida = motor.processar_mensagens(entrada)
        eventos = [
            mensagem["payload"]
            for mensagem in saida
            if mensagem["schema"] == "ods.s2.event"
        ]

        self.assertEqual(
            [evento["phase"] for evento in eventos],
            ["STARTED", "ENDED"],
        )

    def test_reidentifica_track_id_novo_dentro_do_ttl_por_proximidade(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)

        def entrada_com_track(frame: int, ts: int, track_id: str):
            track = {
                "track_id": track_id, "detection_id": 0, "class": "person",
                "state": "CONFIRMED", "predicted": False, "confidence": 0.9,
                "point_px": [100, 200], "bbox": [90, 100, 110, 200],
            }
            pos = {"track_id": track_id, "class": "person", "point_px": [100, 200], "zone_ids": []}
            return [
                envelope("ods.i2.tracks", "cam0", frame, ts, {"frame_index": frame, "tracks": [track]}, "I2"),
                envelope("ods.s1.position", "cam0", frame, ts, {"frame_index": frame, "positions": [pos]}, "S1"),
            ]

        saida = motor.processar_mensagens(
            entrada_com_track(1, BASE, "track-antigo")
            + entrada_com_track(2, BASE + 3000, "track-novo")
        )
        entidades = [e for m in saida if m["schema"] == "ods.s2.entity" for e in m["payload"]["entities"]]
        self.assertEqual(entidades[0]["entity_id"], entidades[1]["entity_id"])

    def test_evento_encerra_depois_do_ttl_sem_observacao(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        motor.carregar_regras(rules(min_duration_ms=0, debounce_ms=0))
        entrada = (
            frame_messages(1, BASE)
            + frame_messages(2, BASE + 1000, sources=("cam0",))
            + frame_messages(3, BASE + 6001, sources=("cam0",))
        )
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertEqual([e["phase"] for e in eventos], ["STARTED", "ENDED"])

    def test_envelope_entity_e_evento_seguem_schemas_da_6c(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0))
        saida = motor.processar_mensagens(frame_messages(1, BASE))
        entidade = next(m for m in saida if m["schema"] == "ods.s2.entity")
        item = entidade["payload"]["entities"][0]
        for campo in ("entity_id", "class", "zone_ids", "source_ids", "track_refs"):
            self.assertIn(campo, item)
        self.assertIn("message_id", entidade)
        self.assertIn("captured_at", entidade)



    def test_i9_rejeita_campos_obrigatorios_invalidos(self):
        casos_invalidos = [
            ("bbox ausente", "bbox", None),
            ("calibrated inválido", "calibrated", "false"),
            ("catalog_version vazia", "catalog_version", ""),
        ]

        for descricao, campo, valor in casos_invalidos:
            with self.subTest(caso=descricao):
                motor = MotorGerenciamentoEstadoConcorrente()
                motor.carregar_regras(rules(min_duration_ms=0))

                payload = {
                    "frame_index": 1,
                    "detection_id": 0,
                    "bbox": [90, 100, 110, 200],
                    "calibrated": False,
                    "catalog_version": "ppe-catalog-1",
                    "uncertain": False,
                    "items": [
                        {
                            "equipment_item": "luva",
                            "compliance": "ABSENT",
                            "confidence": 0.7,
                        }
                    ],
                }

                if valor is None:
                    payload.pop(campo)
                else:
                    payload[campo] = valor

                entrada = frame_messages(1, BASE, sources=("cam0",))
                entrada.append(
                    envelope(
                        "ods.i9.ppe_compliance",
                        "cam0",
                        1,
                        BASE,
                        payload,
                        "I9",
                    )
                )

                with self.assertRaises(ValueError):
                    motor.processar_mensagens(entrada)

if __name__ == "__main__":
    unittest.main()
