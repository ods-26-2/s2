"""Ingestão do S1: transforma as mensagens de I2/I3 em `TrajectoryPoint`.

Reúne o parsing dos envelopes de mensagem, a projeção pixel -> mundo
via homografia, o registro de calibrações por câmera e o carregamento
dos artefatos JSON de entrada (zonas, eventos de I2, calibrações de I3).

Cada mensagem de I2/I3 chega como um envelope comum:

    {
      "message_type": "event",
      "schema": "ods.inferencia.rastreio" | "ods.inferencia.calibracao",
      "schema_version": "1.0",
      "producer": "I2" | "I3",
      "published_at": "<ISO-8601>",
      "payload": {...}
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from .domain import Point, TrajectoryPoint, Zone

PathLike = Union[str, Path]

I2_TRACK_SCHEMA = "ods.inferencia.rastreio"
I3_CALIBRATION_SCHEMA = "ods.inferencia.calibracao"

def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

# --------------------------------------------------------------------------
# Parsing e validação dos envelopes de I2 (rastreio) e I3 (calibração).
# --------------------------------------------------------------------------

class SchemaMismatchError(ValueError):
    """A mensagem recebida não corresponde ao schema esperado."""

def _assert_schema(message: Dict, expected_schema: str) -> None:
    schema = message.get("schema")
    if schema != expected_schema:
        raise SchemaMismatchError(
            f"esperado schema {expected_schema!r}, recebido {schema!r}"
        )

@dataclass(frozen=True)
class RawTrack:
    """Um track dentro do payload de um evento de rastreio (I2)."""

    track_id: int
    object_class: str
    state: str
    u_px: float
    v_px: float
    predicted: bool

@dataclass(frozen=True)
class TrackEvent:
    """Payload de um evento `ods.inferencia.rastreio` (I2), já validado."""

    camera_id: str
    session_id: str
    captured_at: datetime
    frame: Optional[int]
    tracks: List[RawTrack]
    published_at: datetime

@dataclass(frozen=True)
class ReferenceFrame:
    space_id: str
    origin: str
    axes: str
    unit: str

@dataclass(frozen=True)
class CalibrationEvent:
    """Payload de um evento `ods.inferencia.calibracao` (I3), já validado."""

    camera_id: str
    calibration_version: str
    valid_from: datetime
    homography: List[List[float]]
    reference_frame: ReferenceFrame
    reprojection_rms_cm: float
    holdout_points: int
    published_at: datetime

def parse_track_event(message: Dict) -> TrackEvent:
    _assert_schema(message, I2_TRACK_SCHEMA)
    payload = message["payload"]
    tracks = [
        RawTrack(
            track_id=track["track_id"],
            object_class=track["class"],
            state=track["state"],
            u_px=track["u_px"],
            v_px=track["v_px"],
            predicted=track["predicted"],
        )
        for track in payload["tracks"]
    ]
    return TrackEvent(
        camera_id=payload["camera_id"],
        session_id=payload["session_id"],
        captured_at=_parse_timestamp(payload["captured_at"]),
        frame=payload.get("frame"),
        tracks=tracks,
        published_at=_parse_timestamp(message["published_at"]),
    )

def parse_calibration_event(message: Dict) -> CalibrationEvent:
    _assert_schema(message, I3_CALIBRATION_SCHEMA)
    payload = message["payload"]
    reference_frame = payload["reference_frame"]
    return CalibrationEvent(
        camera_id=payload["camera_id"],
        calibration_version=payload["calibration_version"],
        valid_from=_parse_timestamp(payload["valid_from"]),
        homography=payload["homography"],
        reference_frame=ReferenceFrame(
            space_id=reference_frame["space_id"],
            origin=reference_frame["origin"],
            axes=reference_frame["axes"],
            unit=reference_frame["unit"],
        ),
        reprojection_rms_cm=payload["reprojection_rms_cm"],
        holdout_points=payload["holdout_points"],
        published_at=_parse_timestamp(message["published_at"]),
    )

# --------------------------------------------------------------------------
# Homografia: projeção de pixel para coordenadas de mundo.
# [x', y', w']ᵀ = H · [u, v, 1]ᵀ, ponto final em (x'/w', y'/w').
# --------------------------------------------------------------------------

class DegenerateHomographyError(ValueError):
    """A homografia projeta o ponto para o infinito (w = 0)."""

class Homography:
    def __init__(self, matrix: List[List[float]]):
        if len(matrix) != 3 or any(len(row) != 3 for row in matrix):
            raise ValueError("homografia deve ser uma matriz 3x3")
        self._matrix = matrix

    def project(self, u_px: float, v_px: float) -> Point:
        h = self._matrix
        x = h[0][0] * u_px + h[0][1] * v_px + h[0][2]
        y = h[1][0] * u_px + h[1][1] * v_px + h[1][2]
        w = h[2][0] * u_px + h[2][1] * v_px + h[2][2]
        if w == 0:
            raise DegenerateHomographyError(
                f"homografia degenerada: w=0 para o pixel ({u_px}, {v_px})"
            )
        # Arredonda para evitar ruído de ponto flutuante (ex: 50.900000000000006)
        # na saída publicada, sem perda de precisão prática (unidade: metros).
        return Point(round(x / w, 6), round(y / w, 6))

# --------------------------------------------------------------------------
# Registro de calibrações por câmera. I3 pode publicar uma nova versão
# ao longo do tempo (recalibração); para cada leitura de I2, resolvemos
# qual calibração estava vigente na câmera no instante da captura (a de
# `valid_from` mais recente que ainda seja `<= captured_at`).
# --------------------------------------------------------------------------

class CalibrationNotFoundError(LookupError):
    """Nenhuma calibração vigente foi encontrada para a câmera/instante."""

@dataclass(frozen=True)
class Calibration:
    camera_id: str
    calibration_version: str
    valid_from: datetime
    homography: Homography
    reference_frame: ReferenceFrame

class CalibrationRegistry:
    def __init__(self) -> None:
        self._by_camera: Dict[str, List[Calibration]] = {}

    def register(self, event: CalibrationEvent) -> None:
        calibration = Calibration(
            camera_id=event.camera_id,
            calibration_version=event.calibration_version,
            valid_from=event.valid_from,
            homography=Homography(event.homography),
            reference_frame=event.reference_frame,
        )
        entries = self._by_camera.setdefault(event.camera_id, [])
        entries.append(calibration)
        entries.sort(key=lambda c: c.valid_from)

    def register_many(self, events: List[CalibrationEvent]) -> None:
        for event in events:
            self.register(event)

    def resolve(self, camera_id: str, at: datetime) -> Calibration:
        current: Optional[Calibration] = None
        for calibration in self._by_camera.get(camera_id, []):
            if calibration.valid_from <= at:
                current = calibration
            else:
                break
        if current is None:
            raise CalibrationNotFoundError(
                f"nenhuma calibração vigente para camera_id={camera_id!r} em {at.isoformat()}"
            )
        return current

# --------------------------------------------------------------------------
# Ponte I2 + calibração -> TrajectoryPoint (mundo). O track_id de I2 é
# local à câmera — duas câmeras podem reportar track_ids iguais para
# entidades físicas diferentes (ou o inverso: a mesma entidade física
# vista por duas câmeras com track_ids distintos).
# --------------------------------------------------------------------------

def to_trajectory_points(
    event: TrackEvent, calibration_registry: CalibrationRegistry
) -> List[TrajectoryPoint]:
    calibration = calibration_registry.resolve(event.camera_id, event.captured_at)
    return [
        TrajectoryPoint(
            timestamp=event.captured_at,
            source_id=event.camera_id,
            entity_id=f"{event.camera_id}:{track.track_id}",
            position=calibration.homography.project(track.u_px, track.v_px),
        )
        for track in event.tracks
    ]

# --------------------------------------------------------------------------
# Carregamento dos artefatos JSON de entrada.
# --------------------------------------------------------------------------

def load_zones(path: PathLike) -> List[Zone]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    zones = []
    for raw in data["zonas"]:
        polygon = [Point(vertex["x"], vertex["y"]) for vertex in raw["poligono"]]
        zones.append(
            Zone(
                zone_id=raw["zone_id"],
                nome_amigavel=raw["nome_amigavel"],
                polygon=polygon,
                risco=raw["risco"],
                cor_dashboard=raw["cor_dashboard"],
            )
        )
    return zones

def load_track_events(path: PathLike) -> List[TrackEvent]:
    """Carrega uma lista de eventos `ods.inferencia.rastreio` (I2).

    Em produção, cada evento chegaria individualmente via um barramento
    de mensagens; para simulação/testes locais, representamos o fluxo
    como um array JSON de envelopes de mensagem.
    """

    data = json.loads(Path(path).read_text(encoding="utf-8"))
    events = [parse_track_event(raw) for raw in data]
    events.sort(key=lambda event: event.captured_at)
    return events

def load_calibration_events(path: PathLike) -> List[CalibrationEvent]:
    """Carrega uma lista de eventos `ods.inferencia.calibracao` (I3)."""

    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [parse_calibration_event(raw) for raw in data]
