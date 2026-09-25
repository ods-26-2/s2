"""
Módulo: Motor de Gerenciamento de Estado Concorrente
Sprint: 2

Responsabilidades atendidas nesta Sprint:
- S2: Consolidação inicial: criar a estrutura do dicionário em memória.
- S2: Fusão de identidade e Deduplicação: codificar a correlação para unificar IDs.
- S2: Empacotamento Canónico: formatar a saída final no schema oficial da arquitetura.
"""

import math
from typing import Dict, Any, List
from datetime import datetime, timezone

class MotorGerenciamentoEstadoConcorrente:
    def __init__(self, merge_distance_threshold: float = 2.0, time_sync_threshold_ms: int = 100):
        self.merge_distance_threshold = merge_distance_threshold
        self.time_sync_threshold_ms = time_sync_threshold_ms
        self.estado_em_memoria = {}
        self.global_id_counter = 1

    def atualizar_estado_memoria(self, evento: Dict[str, Any]):
        """Registra o evento original na memória."""
        entity_id = evento.get("entity_id")
        self.estado_em_memoria[entity_id] = evento

    def fusao_e_deduplicacao(self, eventos_validos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aplica a regra de proximidade temporal e espacial, devolvendo o envelope S2."""
        eventos_canonicos = []
        indices_processados = set()
        
        for i, ev_base in enumerate(eventos_validos):
            self.atualizar_estado_memoria(ev_base)
            
            if i in indices_processados:
                continue
                
            # Extrai coordenadas de forma segura (lida com chaves aninhadas do Mock)
            coords_base = ev_base.get("world_coordinates", {})
            x_base = coords_base.get("x") if coords_base else ev_base.get("x", 0.0)
            y_base = coords_base.get("y") if coords_base else ev_base.get("y", 0.0)
            fonte_base = ev_base.get("source_id") or ev_base.get("source", "desconhecida")
            
            # Constrói apenas a secção de dados (Payload)
            payload = {
                "global_entity_id": f"GLOBAL_{self.global_id_counter}",
                "fontes_origem": [fonte_base],
                "x": x_base,
                "y": y_base,
                "timestamp": ev_base.get("timestamp"),
                "status": "CONSOLIDADO"
            }
            
            if "zone_id" in ev_base:
                payload["zone_id"] = ev_base.get("zone_id")
                
            self.global_id_counter += 1
            indices_processados.add(i)
            
            for j, ev_comparacao in enumerate(eventos_validos):
                if j in indices_processados:
                    continue
                    
                diff_tempo = abs(ev_base.get("timestamp", 0) - ev_comparacao.get("timestamp", 0))
                
                coords_comp = ev_comparacao.get("world_coordinates", {})
                x_comp = coords_comp.get("x") if coords_comp else ev_comparacao.get("x", 0.0)
                y_comp = coords_comp.get("y") if coords_comp else ev_comparacao.get("y", 0.0)
                
                distancia = math.hypot(x_base - x_comp, y_base - y_comp)
                
                # Se for o mesmo alvo, junta as fontes
                if diff_tempo <= self.time_sync_threshold_ms and distancia <= self.merge_distance_threshold:
                    fonte_comp = ev_comparacao.get("source_id") or ev_comparacao.get("source", "desconhecida")
                    payload["fontes_origem"].append(fonte_comp)
                    indices_processados.add(j)
                    self.atualizar_estado_memoria(ev_comparacao)
            
            # --- EMPACOTAMENTO CANÓNICO ---
            agora_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            
            evento_final_s2 = {
                "message_type": "event",
                "schema": "ods.visao.evento_canonico",
                "schema_version": "1.0",
                "producer": "S2",
                "published_at": agora_iso,
                "payload": payload
            }
            
            eventos_canonicos.append(evento_final_s2)
            
        return eventos_canonicos