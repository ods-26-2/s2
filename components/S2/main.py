import json
import os

from datetime import datetime

# Importa os teus dois componentes
from filtro_ruido_temporal import FiltroRuidoTemporal
from motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente
from config import TREINO_MOT20_ESCOLHIDO

class CamadaS2:
    def __init__(self):
        # A máquina inicializa os dois componentes vazios
        self.filtro_temporal = FiltroRuidoTemporal()
        self.motor_estado = MotorGerenciamentoEstadoConcorrente()

    def processar_eventos(self, fluxo_bruto: list) -> list:
        """
        AQUI ESTÁ A LINHA DE MONTAGEM (PIPELINE):
        """
        # 1. O dado passa primeiro pelo Filtro de Ruído Temporal (Debounce)
        eventos_sem_ruido = self.filtro_temporal.processar(fluxo_bruto)
        
        # 2. Apenas os que sobreviveram vão para o Motor de Fusão
        eventos_canonicos_finais = self.motor_estado.fusao_e_deduplicacao(eventos_sem_ruido)
        
        # 3. Devolve a verdade final consolidada
        return eventos_canonicos_finais

if __name__ == "__main__":
    # Caminhos para os ficheiros originais de entrada
    caminho_eventos_s1 = F"data/s1_evento_espacial_MOT20-0{TREINO_MOT20_ESCOLHIDO}.json"
    caminho_eventos_fusao = F"data/fusao_deduplicacao_MOT20-0{TREINO_MOT20_ESCOLHIDO}.json"

    def executar_teste_com_ficheiro(caminho_ficheiro):
        # O Motor é instanciado DENTRO da função para zera a memória a cada teste
        s2 = CamadaS2()
        
        if os.path.exists(caminho_ficheiro):
            print(f"\n=== A processar o ficheiro: {caminho_ficheiro} ===")
            
            with open(caminho_ficheiro, 'r', encoding='utf-8') as ficheiro:
                fluxo_bruto = json.load(ficheiro)
            
            if isinstance(fluxo_bruto, dict):
                fluxo_bruto = [fluxo_bruto]
                
            # --- 1. NORMALIZAÇÃO DOS DADOS ---
            eventos_normalizados = []
            for ev in fluxo_bruto:
                ts_str = ev.get("timestamp")
                if not ts_str and "transition" in ev:
                    ts_str = ev["transition"].get("timestamp")
                    
                ts_ms = 0
                if ts_str:
                    # Suporta tanto texto ISO-8601 quanto inteiros gerados pelo MOT20
                    if isinstance(ts_str, int):
                        ts_ms = ts_str
                    elif isinstance(ts_str, str):
                        dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                        ts_ms = int(dt.timestamp() * 1000)
                        
                coords = ev.get("world_coordinates", {})
                x = ev.get("x", coords.get("x"))
                y = ev.get("y", coords.get("y"))
                
                eventos_normalizados.append({
                    "entity_id": ev.get("entity_id"),
                    "source": ev.get("source_id", "desconhecida"),
                    "timestamp": ts_ms,
                    "x": x,
                    "y": y,
                    "zone_id": ev.get("zone_id")
                })
            
            # --- 2. EXECUÇÃO DO PIPELINE S2 ---
            eventos_consolidados = s2.processar_eventos(eventos_normalizados)
            
            # --- 3. GERAÇÃO DO FICHEIRO DE SAÍDA JSON ---
            nome_ficheiro_entrada = os.path.basename(caminho_ficheiro)
            caminho_saida = f"data/output_s2/saida_{nome_ficheiro_entrada}"
            
            with open(caminho_saida, 'w', encoding='utf-8') as ficheiro_saida:
                json.dump(eventos_consolidados, ficheiro_saida, indent=4, ensure_ascii=False)
                
            print(f"-> Sucesso! Ficheiro final guardado em: {caminho_saida}")
            
        else:
            print(f"\n[AVISO] O ficheiro '{caminho_ficheiro}' não foi encontrado.")

    # Executa de forma independente para cada cenário
    executar_teste_com_ficheiro(caminho_eventos_s1)
    executar_teste_com_ficheiro(caminho_eventos_fusao)