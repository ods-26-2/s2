"""
Módulo: Filtro de Ruído Temporal
Sprint: 2

Responsabilidade atendida nesta Sprint:
- S2: Janela e debounce: limitar a taxa de eventos repetidos.
"""

from typing import Dict, Any, List

class FiltroRuidoTemporal:
    def __init__(self, debounce_window_ms: int = 500):
        self.debounce_window_ms = debounce_window_ms
        self.ultimo_visto = {}

    def aplicar_debounce(self, evento: Dict[str, Any]) -> bool:
        """
        Limita a taxa de eventos repetidos aplicando uma janela de debounce.
        """
        entity_id = evento.get("entity_id")
        timestamp = evento.get("timestamp")
        
        if entity_id in self.ultimo_visto:
            diferenca = timestamp - self.ultimo_visto[entity_id]
            if diferenca < self.debounce_window_ms:
                return False
                
        self.ultimo_visto[entity_id] = timestamp
        return True

    def processar(self, fluxo_eventos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Recebe múltiplos fluxos de detecções e inferências brutas e aplica o filtro temporal.
        """
        eventos_filtrados = []
        for evento in fluxo_eventos:
            if self.aplicar_debounce(evento):
                eventos_filtrados.append(evento)
        return eventos_filtrados