"""Simulador do componente S1 - Zonas, ocupação e eventos.

Consome eventos de rastreio de I2 (`ods.inferencia.rastreio`, coordenadas
em pixel) e de calibração de I3 (`ods.inferencia.calibracao`, homografia
por câmera), projeta cada track para coordenadas de mundo e imprime, na
ordem temporal, os eventos espacialmente qualificados emitidos pelo S1.
Serve como o "cliente" mínimo para a demonstração da Sprint 2: mostra que
a histerese evita múltiplos eventos de entrada/saída quando uma entidade
oscila na borda de uma zona.

Também grava, ao final, um arquivo JSON com as mensagens publicadas pelo
S1 (`ods.visao.evento_espacial`) — uma por leitura processada.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from S1.domain import S1Service, ZoneResolver
from S1.ingestion import (
    CalibrationRegistry,
    load_calibration_events,
    load_track_events,
    load_zones,
    to_trajectory_points,
)
from S1.output import to_spatial_event

DATA_DIR = Path(__file__).parent.parent.parent / "data"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zonas",
        default=str(DATA_DIR / "definicao_zonas.json"),
        help="Caminho do artefato JSON de definição de zonas.",
    )
    parser.add_argument(
        "--eventos-i2",
        default=str(DATA_DIR / "i2_eventos.json"),
        help="Caminho do JSON com eventos de rastreio de I2 (ods.inferencia.rastreio).",
    )
    parser.add_argument(
        "--calibracoes-i3",
        default=str(DATA_DIR / "i3_calibracoes.json"),
        help="Caminho do JSON com eventos de calibração de I3 (ods.inferencia.calibracao).",
    )
    parser.add_argument(
        "--min-streak",
        type=int,
        default=2,
        help="Nº de leituras consecutivas para confirmar uma transição de zona (janela de histerese).",
    )
    parser.add_argument(
        "--saida",
        default=str(DATA_DIR / "output" / "eventos_s1.json"),
        help="Caminho do JSON com as mensagens publicadas pelo S1 (use '-' para não gravar arquivo).",
    )
    args = parser.parse_args()

    zones = load_zones(args.zonas)

    calibration_registry = CalibrationRegistry()
    calibration_registry.register_many(load_calibration_events(args.calibracoes_i3))

    trajectory = []
    for track_event in load_track_events(args.eventos_i2):
        trajectory.extend(to_trajectory_points(track_event, calibration_registry))
    trajectory.sort(key=lambda point: point.timestamp)

    service = S1Service(ZoneResolver(zones), min_streak=args.min_streak)

    print(f"Carregadas {len(zones)} zona(s) e {len(trajectory)} leitura(s) de trajetória.")
    print(f"Janela de histerese: {args.min_streak} leitura(s) consecutiva(s).\n")

    spatial_events = []
    for point in trajectory:
        event = service.process(point)
        zone_label = event.zone_name or "fora de qualquer zona"
        print(
            f"[{point.timestamp.isoformat()}] {point.entity_id} "
            f"@ ({point.position.x:.2f}, {point.position.y:.2f}) -> {zone_label} "
            f"| evento={event.event_type} | ocupação={event.occupancy}"
        )
        spatial_events.append(to_spatial_event(point, event))

    print("\nOcupação final por zona:")
    for zone in zones:
        count = service.occupancy_snapshot()[zone.zone_id]
        print(f"  {zone.nome_amigavel} ({zone.zone_id}, {zone.risco}): {count} entidade(s)")

    print(f"\n{len(spatial_events)} mensagem(ns) 'ods.visao.evento_espacial' publicada(s).")
    if args.saida != "-":
        output_path = Path(args.saida)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(spatial_events, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Gravado em: {output_path}")


if __name__ == "__main__":
    main()
