"""
Módulo: Pipeline Principal da Camada S2
Sprint: Adequação à Versão 6c

Responsabilidades atendidas:
- Orquestração do pipeline completo (Filtro Temporal -> Motor de Estado Concorrente).
- Normalização de dados brutos e suporte ao comando de regras dinâmicas.
- Exportação de saídas compatíveis com os perfis EV-SINAL e EV-SERVIÇO.
"""

import json
import os
from datetime import datetime

from filtro_ruido_temporal import FiltroRuidoTemporal
from motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente

class CamadaS2:
    def __init__(self):
        self.filtro_temporal = FiltroRuidoTemporal()
        self.motor_estado = MotorGerenciamentoEstadoConcorrente()

    def processar_comando_regras(self, comando_bruto: dict) -> dict:
        """Processa o comando textual de regras ods.s2.rules.command."""
        return self.motor_estado.processar_comando_regras(comando_bruto)

    def processar_eventos(self, fluxo_bruto: list, session_id: str = "default_session") -> list:
        """
        Linha de montagem (Pipeline) da Camada S2:
        1. Filtro de Ruído Temporal (Debounce)
        2. Motor de Fusão, Deduplicação e Envelopamento Canônico v6c
        """
        eventos_sem_ruido = self.filtro_temporal.processar(fluxo_bruto)
        eventos_canonicos_finais = self.motor_estado.fusao_e_deduplicacao(eventos_sem_ruido, session_id=session_id)
        return eventos_canonicos_finais

if __name__ == "__main__":
    caminho_saida_s1 = "data/output/eventos_s1.json"

    def executar_teste_com_ficheiro(caminho_ficheiro):
        s2 = CamadaS2()
        
        if os.path.exists(caminho_ficheiro):
            print(f"\n=== Processando a entrada v6c: {caminho_ficheiro} ===")
            
            with open(caminho_ficheiro, 'r', encoding='utf-8') as ficheiro:
                conteudo_bruto = json.load(ficheiro)
            
            if isinstance(conteudo_bruto, dict):
                conteudo_bruto = [conteudo_bruto]
                
            # Verifica se há comandos de regras no fluxo de entrada
            eventos_para_processar = []
            for item in conteudo_bruto:
                if item.get("schema") == "ods.s2.rules.command" or item.get("message_type") == "command":
                    resposta_reply = s2.processar_comando_regras(item)
                    print(f"-> Comando de regras processado. Reply gerada: {resposta_reply['schema']}")
                else:
                    eventos_para_processar.append(item)
                
            # --- 1. NORMALIZAÇÃO DOS DADOS PARA V6C ---
            eventos_normalizados = []
            for ev in eventos_para_processar:
                payload = ev.get("payload", ev)
                
                ts_str = payload.get("timestamp")
                if not ts_str and "transition" in payload:
                    ts_str = payload["transition"].get("timestamp")
                    
                ts_ms = 0
                if ts_str:
                    if isinstance(ts_str, int):
                        ts_ms = ts_str
                    elif isinstance(ts_str, str):
                        try:
                            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                            ts_ms = int(dt.timestamp() * 1000)
                        except ValueError:
                            ts_ms = int(datetime.now().timestamp() * 1000)
                        
                coords = payload.get("world_coordinates", {})
                x = payload.get("x", coords.get("x", 0.0))
                y = payload.get("y", coords.get("y", 0.0))
                
                eventos_normalizados.append({
                    "entity_id": payload.get("entity_id") or payload.get("global_entity_id"),
                    "source_id": payload.get("source_id") or payload.get("source", "desconhecida"),
                    "timestamp": ts_ms,
                    "x": x,
                    "y": y,
                    "zone_id": payload.get("zone_id"),
                    "frame_index": payload.get("frame_index", 0),
                    "detection_id": payload.get("detection_id", "det-01"),
                    "confidence": payload.get("confidence", 1.0),
                    "phase": payload.get("phase", "STARTED"),
                    "epi_compliance": payload.get("epi_compliance", {}),
                    "catalog_version": payload.get("catalog_version") or payload.get("epi_compliance", {}).get("catalog_version", "v1.0.0")
                })
            
            # --- 2. EXECUÇÃO DO PIPELINE S2 ---
            session_id_atual = "sessao-s2-v6c-01"
            eventos_consolidados = s2.processar_eventos(eventos_normalizados, session_id=session_id_atual)
            
            # --- 3. GERAÇÃO DO FICHEIRO DE SAÍDA JSON ---
            os.makedirs("data/output_s2", exist_ok=True)
            caminho_saida = "data/output_s2/saida_s2_v6c.json"
            
            with open(caminho_saida, 'w', encoding='utf-8') as ficheiro_saida:
                json.dump(eventos_consolidados, ficheiro_saida, indent=4, ensure_ascii=False)
                
            print(f"-> Sucesso! Ficheiro final v6c guardado em: {caminho_saida}")
            
        else:
            print(f"\n[AVISO] O ficheiro '{caminho_ficheiro}' não foi encontrado.")

    executar_teste_com_ficheiro(caminho_saida_s1)
