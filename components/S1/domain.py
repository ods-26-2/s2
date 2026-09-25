"""
Núcleo do componente S1 - Zonas, ocupação e eventos (Camada 3).

Reúne os modelos de dados, o motor de geometria (associação
ponto -> zona), o controlador de histerese (entrada/saída/permanência)
e o serviço que orquestra os três. 
Isso independe de onde os dados entram (`s1/ingestion.py`) ou de como a saída é publicada (`s1/output.py`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon

# --------------------------------------------------------------------------
# Modelos
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Point:
    x: float
    y: float

@dataclass(frozen=True)
class Zone:
    zone_id: str
    nome_amigavel: str
    polygon: List[Point]
    risco: str
    cor_dashboard: str

@dataclass(frozen=True)
class TrajectoryPoint:
    timestamp: datetime
    source_id: str
    entity_id: str
    position: Point

@dataclass
class ZoneEvent:
    entity_id: str
    timestamp: datetime
    zone_id: Optional[str]
    zone_name: Optional[str]
    event_type: str  # "enter" | "exit" | "transfer" | "update" | "none"
    entered_at: Optional[datetime] = None
    dwell_seconds: Optional[float] = None
    occupancy: int = 0
    risco: Optional[str] = None
    cor_dashboard: Optional[str] = None
    previous_zone_id: Optional[str] = None

# --------------------------------------------------------------------------
# Motor de Geometria Estática: resolve se um ponto está dentro de um
# polígono (zona), desempatando sobreposições pela zona de centróide
# mais próximo.
# --------------------------------------------------------------------------

class ZoneResolver:
    def __init__(self, zones: List[Zone]):
        self._zones = list(zones)
        self._polygons: Dict[str, ShapelyPolygon] = {
            zone.zone_id: ShapelyPolygon([(p.x, p.y) for p in zone.polygon])
            for zone in self._zones
        }

    def zones(self) -> List[Zone]:
        return list(self._zones)

    def zone_by_id(self, zone_id: str) -> Optional[Zone]:
        for zone in self._zones:
            if zone.zone_id == zone_id:
                return zone
        return None

    def resolve(self, point: Point) -> Optional[Zone]:
        """Retorna a zona que contém o ponto, ou None se estiver fora
        de todas as zonas conhecidas."""

        shapely_point = ShapelyPoint(point.x, point.y)
        candidates = [
            zone
            for zone in self._zones
            if self._polygons[zone.zone_id].contains(shapely_point)
        ]

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        # Sobreposição: desempata pela zona de centróide mais próximo.
        return min(
            candidates,
            key=lambda zone: self._polygons[zone.zone_id].centroid.distance(
                shapely_point
            ),
        )

# --------------------------------------------------------------------------
# Controlador de Estado Espacial: aplica uma janela de histerese à
# transição de zona de cada entidade, evitando que oscilações na borda
# de uma zona (ex: uma entidade "balançando" na porta) gerem múltiplos
# eventos de entrada/saída. Uma transição só é confirmada depois que a
# nova leitura bruta se repete por `min_streak` amostras consecutivas.
# --------------------------------------------------------------------------

@dataclass
class _EntityState:
    confirmed_zone_id: Optional[str] = None
    pending_zone_id: Optional[str] = None
    pending_streak: int = 0

class HysteresisController:
    def __init__(self, min_streak: int = 2):
        if min_streak < 1:
            raise ValueError("min_streak deve ser >= 1")
        self._min_streak = min_streak
        self._states: Dict[str, _EntityState] = {}

    def update(
        self, entity_id: str, raw_zone_id: Optional[str]
    ) -> Tuple[Optional[str], bool]:
        """Processa a leitura bruta de zona para a entidade.

        Retorna uma tupla (zona_confirmada, houve_transicao).
        """

        state = self._states.setdefault(entity_id, _EntityState())

        if raw_zone_id == state.confirmed_zone_id:
            # Confirma a permanência: descarta qualquer transição pendente.
            state.pending_zone_id = None
            state.pending_streak = 0
            return state.confirmed_zone_id, False

        if raw_zone_id == state.pending_zone_id:
            state.pending_streak += 1
        else:
            state.pending_zone_id = raw_zone_id
            state.pending_streak = 1

        if state.pending_streak >= self._min_streak:
            state.confirmed_zone_id = raw_zone_id
            state.pending_zone_id = None
            state.pending_streak = 0
            return state.confirmed_zone_id, True

        return state.confirmed_zone_id, False

    def current_zone(self, entity_id: str) -> Optional[str]:
        state = self._states.get(entity_id)
        return state.confirmed_zone_id if state else None

    def reset(self, entity_id: str) -> None:
        self._states.pop(entity_id, None)

# --------------------------------------------------------------------------
# S1Service: orquestra geometria + histerese + ocupação. Recebe
# trajetórias contínuas (já em coordenadas de mundo — ver
# s1/ingestion.py para a conversão a partir de I2/I3) e emite eventos
# espacialmente qualificados: associação (carimbo) entre a entidade e a
# região, com a transição de bordas protegida pela histerese.
# --------------------------------------------------------------------------

class S1Service:
    def __init__(self, resolver: ZoneResolver, min_streak: int = 2):
        self._resolver = resolver
        self._hysteresis = HysteresisController(min_streak=min_streak)
        self._occupancy: Dict[str, Set[str]] = {
            zone.zone_id: set() for zone in resolver.zones()
        }
        self._entered_at: Dict[Tuple[str, str], datetime] = {}

    def process(self, point: TrajectoryPoint) -> ZoneEvent:
        raw_zone = self._resolver.resolve(point.position)
        raw_zone_id = raw_zone.zone_id if raw_zone else None

        previous_zone_id = self._hysteresis.current_zone(point.entity_id)
        confirmed_zone_id, transitioned = self._hysteresis.update(
            point.entity_id, raw_zone_id
        )

        event_type = "none"
        if transitioned:
            if previous_zone_id is not None:
                self._occupancy[previous_zone_id].discard(point.entity_id)
                self._entered_at.pop((point.entity_id, previous_zone_id), None)
                event_type = "exit" if confirmed_zone_id is None else "transfer"
            if confirmed_zone_id is not None:
                self._occupancy[confirmed_zone_id].add(point.entity_id)
                self._entered_at[(point.entity_id, confirmed_zone_id)] = point.timestamp
                if previous_zone_id is None:
                    event_type = "enter"
        elif confirmed_zone_id is not None:
            event_type = "update"

        zone = self._resolver.zone_by_id(confirmed_zone_id) if confirmed_zone_id else None
        entered_at = (
            self._entered_at.get((point.entity_id, confirmed_zone_id))
            if confirmed_zone_id
            else None
        )
        dwell_seconds = (
            (point.timestamp - entered_at).total_seconds()
            if entered_at is not None
            else None
        )
        occupancy = (
            len(self._occupancy[confirmed_zone_id]) if confirmed_zone_id else 0
        )

        return ZoneEvent(
            entity_id=point.entity_id,
            timestamp=point.timestamp,
            zone_id=confirmed_zone_id,
            zone_name=zone.nome_amigavel if zone else None,
            event_type=event_type,
            entered_at=entered_at,
            dwell_seconds=dwell_seconds,
            occupancy=occupancy,
            risco=zone.risco if zone else None,
            cor_dashboard=zone.cor_dashboard if zone else None,
            previous_zone_id=previous_zone_id,
        )

    def occupancy_snapshot(self) -> Dict[str, int]:
        """Contagem instantânea de entidades confirmadas por zona."""

        return {zone_id: len(entities) for zone_id, entities in self._occupancy.items()}
