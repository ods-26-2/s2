import os
import json

from config import TREINO_MOT20_ESCOLHIDO

def construir_jsons_de_teste(caminho_gt, saida_s1, saida_fusao, limite_frames=30):
    """
    Lê o Ground Truth do MOT20 e gera os payloads para a Sprint 2.
    """

    print(f"Tentando ler o ficheiro: {caminho_gt}")
    if not os.path.exists(caminho_gt):
        print("ALERTA: O ficheiro não existe neste caminho!")

    eventos_s1 = []
    eventos_fusao = []
    
    # O seqinfo.ini do MOT20 indica 25 FPS (40ms por frame)
    ms_por_frame = 40
    ts_base = 1789552800000 
    
    if not os.path.exists(caminho_gt):
        print(f"Erro: Não foi possível encontrar o ficheiro em {caminho_gt}")
        return

    eventos_gerados = 0 
    limite_eventos = 2000 # Define quantos eventos você quer mockar, independentemente do frame

    with open(caminho_gt, 'r') as ficheiro:
        for linha in ficheiro:
            colunas = linha.strip().split(',')
            
            # O formato MOT padrão é: 
            # [frame, id, bb_left, bb_top, bb_width, bb_height, conf, x, y] ou similar,
            # MAS a coluna 7 costuma ser a "Classe".
            
            # No MOT20 (GT), a coluna 7 é a classe, e a coluna 8 é a visibilidade.
            # Classe 1 = Pedestre ativo.
            if len(colunas) >= 8:
                classe = int(colunas[7])
                if classe != 1:
                    continue # Ignora tudo o que não for pessoa (veículos, manchas, etc)
            
            frame = int(colunas[0])
            entity_id = int(colunas[1])
            
            # Ignorar entradas inválidas caso existam
            if entity_id == -1:
                continue
                
            # Extrair coordenadas e calcular o centro do objeto
            x_esq, y_topo = float(colunas[2]), float(colunas[3])
            largura, altura = float(colunas[4]), float(colunas[5])
            x_centro = round(x_esq + (largura / 2), 2)
            y_centro = round(y_topo + (altura / 2), 2)
            
            ts_atual = ts_base + (frame * ms_por_frame)
            
            # 1. Cria o evento da Câmara Principal
            evento_principal = {
                "timestamp": ts_atual,
                "source_id": "cam_frontal",
                "entity_id": f"pedestre_{entity_id}",
                "world_coordinates": {"x": x_centro, "y": y_centro}
            }
            
            eventos_s1.append(evento_principal)
            
            # 2. Cria o evento da Câmara Secundária (simulando sobreposição)
            evento_secundario = {
                "timestamp": ts_atual + 15,
                "source_id": "cam_lateral",
                "entity_id": f"pedestre_{entity_id}",
                "world_coordinates": {
                    "x": round(x_centro + 0.2, 2), 
                    "y": round(y_centro + 0.1, 2)
                }
            }
            
            eventos_fusao.append(evento_principal)
            eventos_fusao.append(evento_secundario)
            
            # Contador de segurança: Para de ler assim que atingir a meta
            eventos_gerados += 1
            if eventos_gerados >= limite_eventos:
                break

    # Escreve os ficheiros nas pastas corretas
    with open(saida_s1, 'w') as f1:
        json.dump(eventos_s1, f1, indent=2)
        
    with open(saida_fusao, 'w') as f2:
        json.dump(eventos_fusao, f2, indent=2)
        
    print(f"Sucesso!")
    print(f"-> {len(eventos_s1)} eventos guardados em {saida_s1}")
    print(f"-> {len(eventos_fusao)} eventos guardados em {saida_fusao}")

# Ajusta o caminho inicial para onde descompactaste a pasta MOT20

construir_jsons_de_teste(
    caminho_gt=f"MOT20/train/MOT20-0{TREINO_MOT20_ESCOLHIDO}/gt/gt.txt",
    saida_s1=f"data/s1_evento_espacial_MOT20-0{TREINO_MOT20_ESCOLHIDO}.json",
    saida_fusao=f"data/fusao_deduplicacao_MOT20-0{TREINO_MOT20_ESCOLHIDO}.json"
)