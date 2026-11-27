from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

RAIZ_PROJETO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ_PROJETO))

from components.S2.motor_estado_concorrente import (
    MotorGerenciamentoEstadoConcorrente,
)
import uuid
from datetime import datetime, timezone

SESSION = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"
BASE = 1_800_000_000_000


def iso(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def envelope(
    schema: str,
    source: str,
    frame: int,
    ts: int,
    payload: dict,
    producer: str,
) -> dict:
    return {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": producer,
        "published_at": iso(ts),
        "captured_at": iso(ts),
        "source_id": source,
        "session_id": SESSION,
        "sequence": frame,
        "artifact_version": "benchmark-1",
        "payload": payload,
    }


def frame_messages(frame: int, ts: int) -> list[dict]:
    mensagens = []

    for index, source in enumerate(("cam0", "cam1")):
        track_id = f"track-{source}"

        track = {
            "track_id": track_id,
            "detection_id": 0,
            "class": "person",
            "state": "CONFIRMED",
            "predicted": False,
            "confidence": 0.9,
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

        mensagens.append(
            envelope(
                "ods.i2.tracks",
                source,
                frame,
                ts,
                {"frame_index": frame, "tracks": [track]},
                "I2",
            )
        )

        mensagens.append(
            envelope(
                "ods.s1.position",
                source,
                frame,
                ts,
                {"frame_index": frame, "positions": [pos]},
                "S1",
            )
        )

    return mensagens

def executar(
    frames: int,
    usar_snapshot: bool = True,
) -> tuple[list[float], list[float]]:
    motor = MotorGerenciamentoEstadoConcorrente()

    # Aquecimento: executa alguns quadros sem contabilizar os tempos.
    for frame in range(1, 11):
        motor.processar_mensagens(
            frame_messages(frame, BASE + frame * 40)
        )

    motor = MotorGerenciamentoEstadoConcorrente()

    tempos_processamento = []
    tempos_snapshot = []

    capturar_original = motor._capturar_estado

    if usar_snapshot:
        def capturar_medido():
            inicio = time.perf_counter()
            snapshot = capturar_original()
            tempos_snapshot.append(
                (time.perf_counter() - inicio) * 1000
            )
            return snapshot

        motor._capturar_estado = capturar_medido
    else:
        motor._capturar_estado = lambda: {}

    for frame in range(1, frames + 1):
        mensagens = frame_messages(
            frame,
            BASE + frame * 40,
        )

        inicio = time.perf_counter()
        motor.processar_mensagens(mensagens)
        tempos_processamento.append(
            (time.perf_counter() - inicio) * 1000
        )

    return tempos_processamento, tempos_snapshot


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--frames", type=int, default=100)
    parser.add_argument("--repeticoes", type=int, default=3)
    args = parser.parse_args()

    if args.frames < 1 or args.repeticoes < 1:
        parser.error("frames e repeticoes devem ser maiores que zero")

    resultados = {
        "Com snapshot": [],
        "Sem snapshot": [],
    }
    tempos_snapshot = []

    for repeticao in range(args.repeticoes):
        ordem = (
            (True, False)
            if repeticao % 2 == 0
            else (False, True)
        )

        print(f"\n===== REPETIÇÃO {repeticao + 1} =====")

        for usar_snapshot in ordem:
            nome = "Com snapshot" if usar_snapshot else "Sem snapshot"

            processamento, snapshots = executar(
                args.frames,
                usar_snapshot=usar_snapshot,
            )

            mediana = statistics.median(processamento)
            resultados[nome].extend(processamento)
            tempos_snapshot.extend(snapshots)

            print(
                f"{nome}: "
                f"{mediana:.3f} ms/quadro"
            )

    com_snapshot = statistics.median(resultados["Com snapshot"])
    sem_snapshot = statistics.median(resultados["Sem snapshot"])

    diferenca = com_snapshot - sem_snapshot
    percentual = (
        diferenca / com_snapshot * 100
        if com_snapshot
        else 0
    )

    print("\n===== RESULTADO FINAL =====")
    print(f"Com snapshot: {com_snapshot:.3f} ms/quadro")
    print(f"Sem snapshot: {sem_snapshot:.3f} ms/quadro")
    print(f"Custo adicional estimado: {diferenca:.3f} ms/quadro")
    print(f"Redução estimada sem snapshot: {percentual:.1f}%")

    if tempos_snapshot:
        print(
            "Snapshot mediano isolado: "
            f"{statistics.median(tempos_snapshot):.3f} ms"
        )

if __name__ == "__main__":
    main()