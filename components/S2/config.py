"""Configuração local do protótipo S2 alinhada à arquitetura ODS 6c.

RULES e PPE_CATALOG são artefatos versionados da aplicação.
Os parâmetros técnicos de fusão ficam separados desses artefatos.
"""

import os
import os
from pathlib import Path


# Diretórios principais.
DIRETORIO_DADOS = Path("data")
DIRETORIO_SAIDA_S2 = DIRETORIO_DADOS / "output_s2"


# Arquivos de entrada e saída.
# Entrada padrão: mensagens produzidas pelo componente S1.
CAMINHO_ENTRADA_S2 = (
    DIRETORIO_DADOS / "output" / "eventos_s1.json"
)

CAMINHO_ENTRADA_S2_MOCK = DIRETORIO_DADOS / "entrada_s2_mock.json"

# Entrada exclusivamente demonstrativa para testes de fusão multicâmera.
CAMINHO_ENTRADA_S2_FUSAO = (
    DIRETORIO_DADOS / "entrada_s2_fusao_mock.json"
)

# Saída padrão do componente S2.
CAMINHO_SAIDA_S2 = (
    DIRETORIO_SAIDA_S2 / "eventos_s2.json"
)


# Artefatos RULES.
CAMINHO_REGRAS_S2 = (
    DIRETORIO_DADOS / "regras_s2.json"
)

CAMINHO_REGRAS_S2_EXEMPLO = (
    Path(__file__).resolve().parent / "regras_s2.exemplo.json"
)


# Artefatos PPE_CATALOG.
CAMINHO_PPE_CATALOG_S2 = (
    DIRETORIO_DADOS / "ppe_catalog.json"
)

CAMINHO_PPE_CATALOG_S2_EXEMPLO = (
    DIRETORIO_DADOS / "ppe_catalog.exemplo.json"
)


# Parâmetros técnicos de fusão multicâmera.
# A associação entre câmeras ocorre somente em coordenadas métricas
# dentro do mesmo space_id.

DISTANCIA_FUSAO_M = 2.0
SINCRONISMO_FUSAO_MS = 100
TTL_ENTIDADE_MS = 5000
REIDENTIFICACAO_PIXELS = 80.0


# Versões dos contratos e artefatos.
SCHEMA_VERSION = "1.0"

ARTIFACT_VERSION_FUSAO = "s2-fusion-1"
ARTIFACT_VERSION_MOCK_I2 = "mot20-gt-mock-1"
ARTIFACT_VERSION_MOCK_S1 = "zones-mot20-mock-1"
ARTIFACT_VERSION_MOCK_CALIBRACAO = "cal-mock-world-1"


# Configurações utilizadas exclusivamente nos mocks.
SESSION_ID_MOCK = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"

TIMESTAMP_BASE_MOCK = "2026-09-23T14:00:00.000Z"

FPS_MOCK = 25


# Escala fictícia usada APENAS no fixture de fusão multicâmera.
# Os dados MOT20 são pixels; esta transformação não é uma calibração real
# da instalação.

ESCALA_PIXEL_PARA_METRO_MOCK = 0.01

DESLOCAMENTO_CAM2_X_MOCK = 0.10

DESLOCAMENTO_CAM2_Y_MOCK = 0.05

# Configurações de integração Kafka.
# Os valores podem ser sobrescritos por variáveis de ambiente.



KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "34.66.235.91:9093",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "ods-s2-consumer",
)

KAFKA_AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest",
)

KAFKA_ENABLE_AUTO_COMMIT = False

KAFKA_POLL_TIMEOUT_SECONDS = float(
    os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", "1.0")
)

KAFKA_BATCH_MAX_MESSAGES = int(
    os.getenv("KAFKA_BATCH_MAX_MESSAGES", "100")
)

KAFKA_FRAME_WAIT_MS = int(
    os.getenv("KAFKA_FRAME_WAIT_MS", "200")
)

KAFKA_INPUT_TOPICS = [
    topic.strip()
    for topic in os.getenv(
        "KAFKA_INPUT_TOPICS",
        (
            "ods.i2.tracks,"
            "ods.i9.ppe_compliance,"
            "ods.s1.position,"
            "ods.s1.zone_transition,"
            "ods.s1.zone_occupancy,"
            "ods.session.started,"
            "ods.s2.rules.command"
        ),
    ).split(",")
    if topic.strip()
]

KAFKA_OUTPUT_TOPICS = {
    "ods.s2.entity": os.getenv(
        "KAFKA_TOPIC_S2_ENTITY",
        "ods.s2.entity",
    ),
    "ods.s2.event": os.getenv(
        "KAFKA_TOPIC_S2_EVENT",
        "ods.s2.event",
    ),
    "ods.s2.rules.reply": os.getenv(
        "KAFKA_TOPIC_S2_RULES_REPLY",
        "ods.s2.rules.reply",
    ),
}