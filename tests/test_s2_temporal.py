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
    def test_persistencia_minima_e_emissao_started_updated(self):
        motor = MotorGerenciamentoEstadoConcorrente(ttl_threshold_ms=5000)
        motor.carregar_regras(rules(min_duration_ms=1000, debounce_ms=500))
        entrada = frame_messages(1, BASE) + frame_messages(2, BASE + 1000) + frame_messages(3, BASE + 1600)
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertEqual([e["phase"] for e in eventos], ["STARTED", "UPDATED"])
        self.assertEqual(eventos[0]["duration_ms"], 1000)
        self.assertEqual(eventos[0]["status"], "CONFIRMED")

    def test_confianca_intermediaria_publica_uncertain(self):
        motor = MotorGerenciamentoEstadoConcorrente()
        motor.carregar_regras(rules(min_duration_ms=0))
        entrada = frame_messages(1, BASE, confidence=0.6)
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertTrue(eventos)
        self.assertEqual(eventos[0]["status"], "UNCERTAIN")

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
                {"frame_index": frame, "detection_id": 0, "items": [
                    {"equipment_item": "luva", "compliance": "ABSENT", "confidence": 0.7}
                ]}, "I9",
            ))
        saida = motor.processar_mensagens(entrada)
        eventos = [m["payload"] for m in saida if m["schema"] == "ods.s2.event"]
        self.assertTrue(eventos)
        self.assertEqual(eventos[0]["event_type"], "PPE_NON_COMPLIANT")
        self.assertEqual(eventos[0]["equipment_item"], "luva")
        self.assertEqual(eventos[0]["status"], "UNCERTAIN")

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


if __name__ == "__main__":
    unittest.main()
