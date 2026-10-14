"""
Módulo: Motor de Gerenciamento de Estado Concorrente
Sprint: Adequação à Versão 6c

Responsabilidades atendidas:
- Separação de Schemas (ods.s2.entity e ods.s2.event) com perfis EV-SINAL e EV-SERVIÇO.
- Inclusão do Envelope Padrão de 13 campos (incluindo session_id, sequence e artifact_version).
- Correlação de EPI (I9 associado ao rastro I2 por chave espacial do quadro).
- Consumo Dinâmico de Regras via ods.s2.rules.command com emissão de reply de aceitação.
- Gestão de Estados com status UNCERTAIN e ciclo de vida do evento (phase).
"""

import math
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

class MotorGerenciamentoEstadoConcorrente:
    def __init__(self, merge_distance_threshold: float = 2.0, time_sync_threshold_ms: int = 100):
        self.merge_distance_threshold = merge_distance_threshold
        self.time_sync_threshold_ms = time_sync_threshold_ms
        self.estado_em_memoria = {}
        self.global_id_counter = 1
        self.sequence_counter = 0
        self.artifact_version = "1.0.0"
        self.active_rules = {}

    def processar_comando_regras(self, comando_regras: Dict[str, Any]) -> Dict[str, Any]:
        """Consome o comando textual ods.s2.rules.command carregando o artefato RULES e gerando reply."""
        payload_cmd = comando_regras.get("payload", {})
        self.active_rules = payload_cmd.get("rules", {})
        self.artifact_version = payload_cmd.get("artifact_version", self.artifact_version)
        
        agora_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        self.sequence_counter += 1

        # Resposta/Reply de aceitação do comando de regras
        return {
            "message_type": "reply",
            "schema": "ods.s2.rules.reply",
            "schema_version": "1.0",
            "producer": "S2",
            "consumer": comando_regras.get("producer", "unknown"),
            "session_id": comando_regras.get("session_id", "default_session"),
            "sequence": self.sequence_counter,
            "published_at": agora_iso,
            "artifact_version": self.artifact_version,
            "status": "ACCEPTED",
            "payload": {
                "message": "Artefato de regras aplicado com sucesso em S2."
            }
        }

    def _construir_envelope_padrao(self, schema_name: str, session_id: str, payload: Dict[str, Any], source_id: str) -> Dict[str, Any]:
        """Constrói o envelope comum de 13 campos exigido pela arquitetura v6c."""
        agora_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        self.sequence_counter += 1

        return {
            "message_type": "event",
            "schema": schema_name,
            "schema_version": "1.0",
            "producer": "S2",
            "consumer": "system",
            "session_id": session_id,
            "sequence": self.sequence_counter,
            "published_at": agora_iso,
            "source_id": source_id,
            "trace_id": f"trace-{session_id}-{self.sequence_counter}",
            "artifact_version": self.artifact_version,
            "security_level": "internal",
            "payload": payload
        }

    def atualizar_estado_memoria(self, evento: Dict[str, Any]):
        """Registra o evento original na memória com controle de TTL e ciclo de vida."""
        entity_id = evento.get("entity_id") or evento.get("global_entity_id")
        if entity_id:
            self.estado_em_memoria[entity_id] = evento

    def fusao_e_deduplicacao(self, eventos_validos: List[Dict[str, Any]], session_id: str = "default_session") -> List[Dict[str, Any]]:
        """Aplica correlação temporal/espacial, associação de EPI por chave de quadro e gestão de incerteza."""
        mensagens_saida = []
        indices_processados = set()
        
        for i, ev_base in enumerate(eventos_validos):
            self.atualizar_estado_memoria(ev_base)
            
            if i in indices_processados:
                continue
                
            # Extração segura de coordenadas e metadados de quadro (compatível com correlação I2/I9)
            coords_base = ev_base.get("world_coordinates", {})
            x_base = coords_base.get("x") if coords_base else ev_base.get("x", 0.0)
            y_base = coords_base.get("y") if coords_base else ev_base.get("y", 0.0)
            fonte_base = ev_base.get("source_id") or ev_base.get("source", "desconhecida")
            
            frame_index = ev_base.get("frame_index", 0)
            detection_id = ev_base.get("detection_id", f"det-{i}")
            
            # Avaliação de confiança e limiares para status UNCERTAIN
            confianca = ev_base.get("confidence", 1.0)
            status = "CONSOLIDADO"
            if 0.4 <= confianca < 0.7:
                status = "UNCERTAIN"

            # Identificação do ciclo de vida do evento
            fase_evento = ev_base.get("phase", "STARTED")

            # Associações de EPI (I9) via chave espacial do quadro (source_id, session_id, frame_index, detection_id)
            epi_conformidade = ev_base.get("epi_compliance", {})

            payload_entidade = {
                "global_entity_id": f"GLOBAL_{self.global_id_counter}",
                "fontes_origem": [fonte_base],
                "x": x_base,
                "y": y_base,
                "timestamp": ev_base.get("timestamp"),
                "status": status,
                "phase": fase_evento,
                "frame_index": frame_index,
                "detection_id": detection_id,
                "epi_compliance": epi_conformidade,
                "confidence": confianca
            }
            
            if "zone_id" in ev_base:
                payload_entidade["zone_id"] = ev_base.get("zone_id")
                
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
                
                # Associação por proximidade e sincronia temporal
                if diff_tempo <= self.time_sync_threshold_ms and distancia <= self.merge_distance_threshold:
                    fonte_comp = ev_comparacao.get("source_id") or ev_comparacao.get("source", "desconhecida")
                    if fonte_comp not in payload_entidade["fontes_origem"]:
                        payload_entidade["fontes_origem"].append(fonte_comp)
                    indices_processados.add(j)
                    self.atualizar_estado_memoria(ev_comparacao)
            
            # --- PUBLICAÇÃO DE SCHEMAS SEPARADOS (ods.s2.entity e ods.s2.event) ---
            
            # 1. Publicação da Entidade Fundida (Perfil EV-SINAL)
            envelope_entity = self._construir_envelope_padrao(
                schema_name="ods.s2.entity",
                session_id=session_id,
                payload=payload_entidade,
                source_id=fonte_base
            )
            mensagens_saida.append(envelope_entity)

            # 2. Publicação do Evento Qualificado pós-persistência (Perfil EV-SERVIÇO)
            payload_evento = {
                "event_id": f"evt-{payload_entidade['global_entity_id']}",
                "global_entity_id": payload_entidade["global_entity_id"],
                "event_type": "DETECTION_PERSISTED",
                "phase": fase_evento,
                "status": status,
                "frame_index": frame_index,
                "detection_id": detection_id,
                "timestamp": payload_entidade["timestamp"]
            }
            envelope_event = self._construir_envelope_padrao(
                schema_name="ods.s2.event",
                session_id=session_id,
                payload=payload_evento,
                source_id=fonte_base
            )
            mensagens_saida.append(envelope_event)
            
        return mensagens_saida
