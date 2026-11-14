
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

SESSION_ID = "a50e8400-e29b-41d4-a716-446655440099"
SOURCE_ID = "cam_frontal"

quadros = [
    ("2026-11-11T15:00:00Z", "INCORRECT"),
    ("2026-11-11T15:00:03Z", "INCORRECT"),
    ("2026-11-11T15:00:06Z", "INCORRECT"),
    ("2026-11-11T15:00:08Z", "INCORRECT"),
    ("2026-11-11T15:00:09Z", "CORRECT"),
]

mensagens = []
sequence = 0

def gerar_uuid(texto):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, texto))

def mais_um_segundo(timestamp):
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return (dt + timedelta(seconds=1)).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")

for frame_index, (captured_at, compliance) in enumerate(quadros, start=100):

    published_at = mais_um_segundo(captured_at)

    mensagem_i2 = {
        "message_type": "event",
        "message_id": gerar_uuid(f"ppe-i2-{frame_index}"),
        "schema": "ods.i2.tracks",
        "schema_version": "1.0",
        "producer": "I2",
        "published_at": published_at,
        "captured_at": captured_at,
        "session_id": SESSION_ID,
        "sequence": sequence,
        "source_id": SOURCE_ID,
        "artifact_version": "i2-test-1",
        "payload": {
            "frame_index": frame_index,
            "tracks": [
                {
                    "track_id": "1",
                    "detection_id": 0,
                    "class": "person",
                    "state": "CONFIRMED",
                    "predicted": False,
                    "confidence": 0.95,
                    "point_px": [505, 702],
                    "bbox": [412, 180, 598, 702]
                }
            ]
        }
    }

    sequence += 1

    mensagem_i9 = {
        "message_type": "event",
        "message_id": gerar_uuid(f"ppe-i9-{frame_index}"),
        "schema": "ods.i9.ppe_compliance",
        "schema_version": "1.0",
        "producer": "I9",
        "published_at": published_at,
        "captured_at": captured_at,
        "session_id": SESSION_ID,
        "sequence": sequence,
        "source_id": SOURCE_ID,
        "artifact_version": "i9-test-1",
        "payload": {
            "frame_index": frame_index,
            "detection_id": 0,
            "uncertain": False,
            "items": [
                {
                    "equipment_item": "luva",
                    "compliance": compliance,
                    "confidence": 0.95
                }
            ]
        }
    }

    sequence += 1

    mensagens.extend([mensagem_i2, mensagem_i9])

caminho = Path("data/entrada_teste_ppe.json")
caminho.parent.mkdir(parents=True, exist_ok=True)

caminho.write_text(
    json.dumps(mensagens, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print(f"Arquivo criado: {caminho}")
print(f"Total de mensagens: {len(mensagens)}")
