"""Configuracao local do prototipo S2 alinhada a arquitetura ODS 6c.

RULES e um artefato versionado da aplicacao. O motor recebe seu conteudo por
carregar_regras(); os parametros tecnicos de fusao ficam separados do artefato.
"""

from pathlib import Path


# Diretorios principais.
DIRETORIO_DADOS = Path("data")
DIRETORIO_SAIDA_S2 = DIRETORIO_DADOS / "output_s2"


# Arquivos de entrada e saida.
CAMINHO_ENTRADA_S2 = (
    DIRETORIO_DADOS / "output" / "eventos_s1.json"
)

# Mantido para compatibilidade com os testes de fusao.
CAMINHO_ENTRADA_S2_FUSAO = CAMINHO_ENTRADA_S2

CAMINHO_SAIDA_S2 = (
    DIRETORIO_SAIDA_S2 / "eventos_s2.json"
)


# Artefatos de regras.
CAMINHO_REGRAS_S2 = (
    DIRETORIO_DADOS / "regras_s2.json"
)

CAMINHO_REGRAS_S2_EXEMPLO = (
    Path(__file__).resolve().parent / "regras_s2.exemplo.json"
)


# Parametros tecnicos de fusao multicamera.
# A associacao entre cameras ocorre somente em coordenadas metricas
# dentro do mesmo space_id.

DISTANCIA_FUSAO_M = 2.0
SINCRONISMO_FUSAO_MS = 100
TTL_ENTIDADE_MS = 5000
REIDENTIFICACAO_PIXELS = 80.0


# Versoes dos contratos e artefatos.
SCHEMA_VERSION = "1.0"

ARTIFACT_VERSION_FUSAO = "s2-fusion-1"
ARTIFACT_VERSION_MOCK_I2 = "mot20-gt-mock-1"
ARTIFACT_VERSION_MOCK_S1 = "zones-mot20-mock-1"
ARTIFACT_VERSION_MOCK_CALIBRACAO = "cal-mock-world-1"


# Configuracoes utilizadas exclusivamente nos mocks.
SESSION_ID_MOCK = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"

TIMESTAMP_BASE_MOCK = "2026-09-23T14:00:00.000Z"

FPS_MOCK = 25


# Escala ficticia usada APENAS no fixture de fusao multicamera.
# Os dados MOT20 sao pixels; esta transformacao nao e uma calibracao real
# da instalacao.

ESCALA_PIXEL_PARA_METRO_MOCK = 0.01

DESLOCAMENTO_CAM2_X_MOCK = 0.10

DESLOCAMENTO_CAM2_Y_MOCK = 0.05
