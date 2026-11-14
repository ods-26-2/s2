
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from uuid import uuid4

ARQUIVO_SAIDA = Path("data/entrada_teste_ppe_debounce.json")
SESSION_ID = str(uuid4())
SOURCE_ID = "cam_frontal"
TRACK_ID = "1"

inicio = datetime(2026, 11, 13, 15, 0, 0, tzinfo=timezone.utc)

mensagens = []

# Cinco quadros: ausência da luva e posterior correção.
status_por_quadro = [
    "ABSENT",
    "ABSENT",
    "ABSENT",
    "ABSENT",
    "ABSENT",
    "CORRECT",
]

for i, status in enumerate(status_por_quadro):
    instante = inicio + timedelta(
        milliseconds=[0, 3000, 6000, 6500, 7000, 8000][i]
    )
    timestamp = instante.isoformat().replace("+00:00", "Z")
    frame_index = 100 + i

    # Mensagem I2: identifica a pessoa.
    mensagens.append({
        "message_id": str(uuid4()),
        "session_id": SESSION_ID,
        "sequence": len(mensagens) + 1,
        "captured_at": timestamp,
        "published_at": timestamp,
        "source_id": SOURCE_ID,
        "producer": "I2",
        "message_type": "event",
        "schema": "ods.i2.tracks",
        "schema_version": "1.0.0",
        "payload": {
            "frame_index": frame_index,
            "tracks": [
                {
                    "track_id": TRACK_ID,
                    "class": "person",
                    "state": "CONFIRMED",
                    "predicted": False,
                    "confidence": 0.95,
                    "detection_id": 0,
                    "point_px": [320, 240],
                    "bbox": [280, 180, 360, 300]
                }
            ]
        }
    })

    # Mensagem I9: informa a situação da luva.
    mensagens.append({
        "message_id": str(uuid4()),
        "session_id": SESSION_ID,
        "sequence": len(mensagens) + 1,
        "captured_at": timestamp,
        "published_at": timestamp,
        "source_id": SOURCE_ID,
        "producer": "I9",
        "message_type": "event",
        "schema": "ods.i9.ppe_compliance",
        "schema_version": "1.0.0",
        "payload": {
            "frame_index": frame_index,
            "detection_id": 0,
            "uncertain": False,
            "items": [
                {
                    "equipment_item": "luva",
                    "compliance": status,
                    "confidence": 0.95
                }
            ]
        }
    })

ARQUIVO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
ARQUIVO_SAIDA.write_text(
    json.dumps(mensagens, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print(f"Teste gerado: {ARQUIVO_SAIDA}")
print(f"Mensagens geradas: {len(mensagens)}")
