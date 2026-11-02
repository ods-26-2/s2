"""Gera fixtures JSON de S1/I2 compativeis com os contratos ODS 6c.

Os arquivos produzidos contem mensagens completas (envelope comum 1.0):
- ods.i2.tracks: rastros por quadro, derivados do GT do MOT20;
- ods.s1.position: posicao por rastro/quadro, com point_px e zone_ids.

O cenário multicamera e explicitamente sintetico. Ele adiciona uma segunda
fonte e position_m em um space_id ficticio apenas para testar a fusao de S2;
nao representa calibracao real nem deve ser usado em producao.
"""
from __future__ import annotations

import argparse
import json
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:  # execução como pacote
    from .config import (
        ARTIFACT_VERSION_MOCK_I2, ARTIFACT_VERSION_MOCK_S1, CAMINHO_ENTRADA_S2,
        CAMINHO_ENTRADA_S2_FUSAO, DESLOCAMENTO_CAM2_X_MOCK, DESLOCAMENTO_CAM2_Y_MOCK,
        ESCALA_PIXEL_PARA_METRO_MOCK, FPS_MOCK, SESSION_ID_MOCK, TIMESTAMP_BASE_MOCK,
        TREINO_MOT20_ESCOLHIDO,
    )
except ImportError:  # execução direta a partir de components/S2
    from config import (
        ARTIFACT_VERSION_MOCK_I2, ARTIFACT_VERSION_MOCK_S1, CAMINHO_ENTRADA_S2,
        CAMINHO_ENTRADA_S2_FUSAO, DESLOCAMENTO_CAM2_X_MOCK, DESLOCAMENTO_CAM2_Y_MOCK,
        ESCALA_PIXEL_PARA_METRO_MOCK, FPS_MOCK, SESSION_ID_MOCK, TIMESTAMP_BASE_MOCK,
        TREINO_MOT20_ESCOLHIDO,
    )

_NAMESPACE_MOCK = uuid.UUID(SESSION_ID_MOCK)


def _parse_timestamp(valor: str) -> datetime:
    dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("TIMESTAMP_BASE_MOCK deve incluir fuso horario")
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _message_id(source: str, schema: str, frame: int) -> str:
    return str(uuid.uuid5(_NAMESPACE_MOCK, f"{source}:{schema}:{frame}"))


def _envelope(
    *, schema: str, producer: str, source_id: str, artifact_version: str,
    frame_index: int, captured_at: str, payload: dict[str, Any], sequence: int,
) -> dict[str, Any]:
    return {
        "message_type": "event",
        "message_id": _message_id(source_id, schema, frame_index),
        "schema": schema,
        "schema_version": "1.0",
        "producer": producer,
        "published_at": captured_at,
        "captured_at": captured_at,
        "source_id": source_id,
        "session_id": SESSION_ID_MOCK,
        "sequence": sequence,
        "artifact_version": artifact_version,
        "payload": payload,
    }


def ler_ground_truth(caminho_gt: Path, limite_frames: int | None = 30, limite_deteccoes: int = 2000) -> dict[int, list[dict[str, Any]]]:
    """Le MOTChallenge GT e agrupa deteccoes por frame, ignorando pessoas invalidas."""
    if not caminho_gt.exists():
        raise FileNotFoundError(f"Ground Truth nao encontrado: {caminho_gt}")
    if limite_frames is not None and limite_frames < 1:
        raise ValueError("limite_frames deve ser >= 1 ou None")
    if limite_deteccoes < 1:
        raise ValueError("limite_deteccoes deve ser >= 1")

    por_frame: dict[int, list[dict[str, Any]]] = defaultdict(list)
    total = 0
    with caminho_gt.open("r", encoding="utf-8") as arquivo:
        for numero_linha, linha in enumerate(arquivo, start=1):
            texto = linha.strip()
            if not texto:
                continue
            colunas = [c.strip() for c in texto.split(",")]
            if len(colunas) < 6:
                raise ValueError(f"Linha {numero_linha} do GT tem menos de 6 colunas")
            try:
                frame = int(float(colunas[0]))
                identity = int(float(colunas[1]))
                x, y, largura, altura = map(float, colunas[2:6])
                # MOT20 contém somente pedestres; seu GT segue o formato
                # MOTChallenge (não possui coluna de classe confiável).
                classe = 1
                confianca = float(colunas[6]) if len(colunas) >= 7 else 1.0
            except ValueError as exc:
                raise ValueError(f"Dados invalidos na linha {numero_linha} do GT") from exc
            # No GT do MOT20, class=1 identifica pedestre; ignore ids invalidos.
            if identity < 0 or classe != 1 or largura <= 0 or altura <= 0:
                continue
            if confianca <= 0:
                continue
            point_px = [round(x + largura / 2, 2), round(y + altura, 2)]
            bbox = [round(x, 2), round(y, 2), round(x + largura, 2), round(y + altura, 2)]
            por_frame[frame].append({
                "track_id": f"t-{identity:04d}",
                "detection_id": len(por_frame[frame]),
                "class": "person",
                "state": "CONFIRMED",
                "predicted": False,
                "confidence": 0.90,
                "point_px": point_px,
                "bbox": bbox,
            })
            total += 1
            if total >= limite_deteccoes:
                break

    if limite_frames is not None:
        frames_permitidos = sorted(por_frame)[:limite_frames]
        por_frame = {frame: por_frame[frame] for frame in frames_permitidos}  # type: ignore[assignment]
    if not por_frame:
        raise ValueError("Nenhuma deteccao de pedestre valida foi encontrada no GT")
    return dict(sorted(por_frame.items()))


def _gerar_mensagens(
    deteccoes_por_frame: dict[int, list[dict[str, Any]]],
    *, multicamera: bool,
) -> list[dict[str, Any]]:
    inicio = _parse_timestamp(TIMESTAMP_BASE_MOCK)
    mensagens: list[dict[str, Any]] = []
    sequencias: dict[str, int] = defaultdict(int)

    fontes = [("cam0", 0)]
    if multicamera:
        fontes.append(("cam1", 15))

    for frame_index, deteccoes in deteccoes_por_frame.items():
        # MOT20 e tipicamente 25 FPS; a base temporal e apenas para replay/teste.
        captured_base = inicio + timedelta(milliseconds=round((frame_index - 1) * 1000 / FPS_MOCK))
        for source_id, offset_ms in fontes:
            captured_at = _iso(captured_base + timedelta(milliseconds=offset_ms))
            tracks: list[dict[str, Any]] = []
            positions: list[dict[str, Any]] = []
            for deteccao in deteccoes:
                track = dict(deteccao)
                # detection_id e local ao quadro/fonte e continua igual entre I2/I9.
                tracks.append(track)
                ponto = track["point_px"]
                posicao: dict[str, Any] = {
                    "track_id": track["track_id"],
                    "class": track["class"],
                    "point_px": list(ponto),
                    "zone_ids": [],
                }
                if multicamera:
                    # Somente fixture de teste: coordenadas metricas ficticias no
                    # mesmo referencial, para exercitar correlacao entre camaras.
                    posicao["position_m"] = [
                        round(float(ponto[0]) * ESCALA_PIXEL_PARA_METRO_MOCK + (DESLOCAMENTO_CAM2_X_MOCK if source_id == "cam1" else 0.0), 3),
                        round(float(ponto[1]) * ESCALA_PIXEL_PARA_METRO_MOCK + (DESLOCAMENTO_CAM2_Y_MOCK if source_id == "cam1" else 0.0), 3),
                    ]
                    posicao["space_id"] = "mock-world-not-calibrated"
                positions.append(posicao)

            # As sequencias sao crescentes por producer + session_id.
            mensagens.append(_envelope(
                schema="ods.i2.tracks", producer="I2", source_id=source_id,
                artifact_version=ARTIFACT_VERSION_MOCK_I2,
                frame_index=frame_index, captured_at=captured_at,
                payload={"frame_index": frame_index, "tracks": tracks},
                sequence=sequencias["I2"],
            ))
            sequencias["I2"] += 1
            mensagens.append(_envelope(
                schema="ods.s1.position", producer="S1", source_id=source_id,
                artifact_version=(ARTIFACT_VERSION_MOCK_S1 if not multicamera else "zones-mock-calibrated-fixture-1"),
                frame_index=frame_index, captured_at=captured_at,
                payload={"frame_index": frame_index, "positions": positions},
                sequence=sequencias["S1"],
            ))
            sequencias["S1"] += 1

    return sorted(mensagens, key=lambda msg: (msg["captured_at"], msg["source_id"], msg["producer"]))


def construir_jsons_de_teste(
    caminho_gt: str | Path,
    saida_s1: str | Path = CAMINHO_ENTRADA_S2,
    saida_fusao: str | Path = CAMINHO_ENTRADA_S2_FUSAO,
    limite_frames: int | None = 30,
    limite_deteccoes: int = 2000,
) -> tuple[int, int]:
    caminho_gt = Path(caminho_gt)
    deteccoes = ler_ground_truth(caminho_gt, limite_frames, limite_deteccoes)
    mensagens_s1 = _gerar_mensagens(deteccoes, multicamera=False)
    mensagens_fusao = _gerar_mensagens(deteccoes, multicamera=True)

    for destino, mensagens in ((Path(saida_s1), mensagens_s1), (Path(saida_fusao), mensagens_fusao)):
        destino.parent.mkdir(parents=True, exist_ok=True)
        with destino.open("w", encoding="utf-8") as arquivo:
            json.dump(mensagens, arquivo, indent=2, ensure_ascii=False)
            arquivo.write("\n")

    print(f"Cenarios gerados a partir de {caminho_gt}")
    print(f"  {len(mensagens_s1)} mensagens (uma camera): {saida_s1}")
    print(f"  {len(mensagens_fusao)} mensagens (duas cameras sinteticas): {saida_fusao}")
    print("  Observacao: a geometria multicamera e ficticia e serve apenas para testar a fusao.")
    return len(mensagens_s1), len(mensagens_fusao)


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera fixtures JSON compativeis com interfaces ODS 6c.")
    parser.add_argument(
        "--gt", type=Path,
        default=Path(f"MOT20/train/MOT20-0{TREINO_MOT20_ESCOLHIDO}/gt/gt.txt"),
        help="Caminho para gt.txt do MOT20",
    )
    parser.add_argument("--output", type=Path, default=CAMINHO_ENTRADA_S2)
    parser.add_argument("--fusion-output", type=Path, default=CAMINHO_ENTRADA_S2_FUSAO)
    parser.add_argument("--frames", type=int, default=30, help="Numero maximo de quadros; 0 significa todos")
    parser.add_argument("--detections", type=int, default=2000, help="Numero maximo de deteccoes validas")
    args = parser.parse_args()
    try:
        construir_jsons_de_teste(
            args.gt, args.output, args.fusion_output,
            limite_frames=None if args.frames == 0 else args.frames,
            limite_deteccoes=args.detections,
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
