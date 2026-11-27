# Componente S2: Regras Temporais e Eventos

## Descrição

Este repositório contém o código e a documentação do componente S2 da arquitetura ODS 6c.

Sua responsabilidade principal é processar mensagens provenientes dos componentes anteriores, aplicar regras temporais, realizar a consolidação e deduplicação de identidades e gerenciar o ciclo de vida dos eventos, produzindo mensagens `ods.s2.entity` e `ods.s2.event` para as próximas camadas do sistema.

Entre suas principais funcionalidades estão:

- Associação de observações às identidades globais;
- Consolidação de rastros provenientes de diferentes câmeras;
- Deduplicação de mensagens;
- Aplicação de regras temporais, incluindo persistência mínima, debounce e TTL;
- Processamento de eventos de permanência em zonas e conformidade de EPIs;
- Classificação de eventos conforme níveis de confiança e incerteza;
- Integração com o barramento Kafka, utilizando processamento por lotes e controle de offsets.

## Estrutura do Repositório

- `components/S2/`: implementação do componente S2, incluindo motor de gerenciamento de estado, filtro temporal, adaptador Kafka, configurações e regras demonstrativas.
- `tests/`: testes automatizados do componente.
- `data/`: arquivos JSON utilizados como entradas, artefatos demonstrativos e saídas nos testes.
- `data/output_s2/`: arquivos JSON produzidos pelo componente.
- `docs/`: documentação arquitetural e documentação complementar.
- `benchmarks/`: scripts de avaliação de desempenho.
- `Dockerfile`: configuração para construção da imagem Docker.
- `.dockerignore`: configuração dos arquivos excluídos do contexto de construção da imagem.
- `docker-compose.yml`: configuração de execução do componente com Docker Compose.
- `pytest.ini`: configuração dos testes automatizados.
- `requirements.txt`: dependências do projeto.
- `RELATORIO_TESTES_S2.md`: relatório dos testes realizados.

**Antes de utilizar este componente, as equipes responsáveis pela integração devem consultar a documentação disponível em `docs/` e os contratos de mensagens da arquitetura 6c.**

## Instruções de Execução Local

O componente pode ser executado localmente por meio de arquivos JSON ou integrado ao barramento Kafka.

### Pré-requisitos

- Python 3.10 ou superior;
- Git;
- Terminal ou prompt de comando.

### 1. Clone o repositório

```bash
git clone https://github.com/ods-26-2/s2.git
cd s2
```

### 2. Crie e ative um ambiente virtual

É recomendado utilizar um ambiente virtual para isolar as dependências do S2.

**Windows:**

```powershell
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instale as dependências

As dependências do projeto estão listadas em `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 4. Execute o componente principal

O S2 recebe um arquivo JSON contendo mensagens dos componentes anteriores e gera um arquivo JSON com as entidades e eventos processados.

Para executar utilizando os caminhos padrão:

```bash
python -m components.S2.main
```

Também é possível informar explicitamente os arquivos de entrada e saída:

```bash
python -m components.S2.main --input data/output/eventos_s1.json --output data/output_s2/saida_s2.json
```

### 5. Configuração dos artefatos

O componente utiliza os artefatos RULES e PPE_CATALOG para determinar as condições de geração dos eventos e os requisitos de conformidade de EPIs.

É possível informar os arquivos explicitamente:

```bash
python -m components.S2.main --input data/entrada.json --output data/output_s2/saida.json --rules data/regras_s2.json --ppe-catalog data/ppe_catalog.json
```

Quando os argumentos não são informados, o componente procura os artefatos nos caminhos padrão e utiliza arquivos demonstrativos como fallback, quando disponíveis.

Os artefatos demonstrativos servem para validação e testes. Para utilização integrada, devem ser fornecidos os artefatos correspondentes ao ambiente.

### Argumentos disponíveis

| Argumento | Descrição |
|---|---|
| `--input` ou `-i` | Caminho do JSON de entrada |
| `--output` ou `-o` | Caminho do JSON de saída |
| `--rules` ou `-r` | Caminho do artefato RULES |
| `--ppe-catalog` | Caminho do artefato PPE_CATALOG |
| `--fusao-mock` | Utiliza a entrada demonstrativa de fusão quando a entrada padrão está selecionada |
| `--kafka` | Executa o componente em modo de consumo e publicação pelo Kafka |

## Integração com Kafka

O componente possui um adaptador Kafka responsável por consumir mensagens dos componentes anteriores, agrupá-las conforme os respectivos quadros e sessões, processá-las pelo motor S2 e publicar os resultados.

### Tópicos de entrada

- `ods.i2.tracks`
- `ods.s1.position`
- `ods.s1.zone_transition`
- `ods.s1.zone_occupancy`
- `ods.i9.ppe_compliance`
- `ods.session.started`
- `ods.s2.rules.command`

### Tópicos de saída

- `ods.s2.entity`
- `ods.s2.event`
- `ods.s2.rules.reply`

### Execução

Para iniciar o componente em modo Kafka:

```bash
python -m components.S2.main --kafka
```

A configuração do broker e dos demais parâmetros pode ser realizada por variáveis de ambiente.

Entre as principais configurações estão:

| Variável | Descrição |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Endereço do broker Kafka |
| `KAFKA_GROUP_ID` | Grupo de consumidores |
| `KAFKA_AUTO_OFFSET_RESET` | Política inicial de leitura dos offsets |
| `KAFKA_INPUT_TOPICS` | Tópicos de entrada |
| `S2_KAFKA_FRAME_WAIT_MS` | Tempo de espera para agrupamento dos quadros |
| `S2_MAX_IDS_DEDUPLICACAO` | Limite de identificadores mantidos no controle de deduplicação |

As configurações com prefixo `S2_KAFKA_` são utilizadas para os parâmetros específicos do adaptador, mantendo compatibilidade com variáveis legadas quando aplicável.

O adaptador utiliza controle manual de offsets. Os offsets são confirmados após a publicação bem-sucedida das respectivas saídas.

**Observação:** o controle de publicação e confirmação de offsets permite retentativas durante a execução, mas não garante processamento exactly-once em todos os cenários. Uma interrupção inesperada entre a publicação e a confirmação dos offsets pode ocasionar duplicidade após a retomada.

A execução em um broker compartilhado deve ocorrer somente após a autorização e a validação dos parâmetros de integração.

## Testes Automatizados

O projeto possui testes automatizados utilizando `pytest`.

Para executar todos os testes:

```bash
pytest -v
```

A configuração `pytest.ini` permite executar os testes diretamente na raiz do projeto.

Os testes verificam:

- Funcionamento do filtro temporal;
- Persistência mínima e debounce;
- Tratamento de confiança e incerteza;
- Correlação entre mensagens dos componentes I2 e I9;
- Compatibilidade dos envelopes com os schemas da arquitetura 6c;
- Ciclo de vida dos eventos;
- Reidentificação de rastros;
- Expiração e renovação de identidades;
- Agrupamento e processamento de mensagens Kafka;
- Publicação e confirmação de offsets;
- Tratamento de falhas e retentativas.

**Última validação registrada:** 78 testes aprovados e 7 subtests aprovados.

Para mais detalhes, consulte `RELATORIO_TESTES_S2.md`.

## Execução com Docker

O projeto permite executar o componente em um contêiner Docker.

### 1. Pré-requisitos

- Docker instalado e em execução.

### 2. Construa a imagem

Na raiz do repositório, execute:

```bash
docker build -t ods-s2:local .
```

### 3. Execute o componente

A pasta `data/` deve ser montada como volume para permitir que o contêiner leia os arquivos de entrada e grave os resultados no ambiente local.

Exemplo:

```bash
docker run --rm -v "$(pwd)/data:/app/data" ods-s2:local --input data/output/eventos_s1.json --output data/output_s2/saida_docker.json
```

No Linux, esse comando compartilha a pasta `data/` do projeto com o contêiner.

Assim, os arquivos de entrada podem ser alterados sem reconstruir a imagem, e os resultados gerados ficam disponíveis em `data/output_s2/`.

No Windows PowerShell, utilize:

```powershell
docker run --rm -v "${PWD}/data:/app/data" ods-s2:local --input data/output/eventos_s1.json --output data/output_s2/saida_docker.json
```

### 4. Execução com Docker Compose

O projeto também possui um arquivo `docker-compose.yml` para configurar a execução do S2 em modo Kafka.

Para validar a configuração:

```bash
docker compose config
```

Para construir a imagem:

```bash
docker compose build
```

Para iniciar o serviço:

```bash
docker compose up -d
```

Para acompanhar os logs:

```bash
docker compose logs -f s2
```

Para interromper o serviço:

```bash
docker compose down
```

**Atenção:** a configuração padrão do Compose utiliza o endereço de broker definido em `KAFKA_BOOTSTRAP_SERVERS`, cujo valor padrão é `34.66.235.91:9093`. Antes de iniciar o serviço, configure o broker e confirme a autorização para consumir e publicar mensagens no ambiente correspondente.

## Fluxo de Execução

O fluxo de execução local pode ser resumido da seguinte forma:

```text
Clone do repositório
        ↓
Criação do ambiente virtual
        ↓
Instalação das dependências
        ↓
Preparação dos artefatos e mensagens de entrada
        ↓
Carregamento das regras
        ↓
Validação e processamento das mensagens
        ↓
Consolidação e deduplicação de identidades
        ↓
Aplicação das regras temporais
        ↓
Geração das entidades e eventos
        ↓
Publicação no Kafka ou gravação do JSON de saída
```

## Estado Atual da Integração

O componente já possui:

- Processamento local por arquivos JSON;
- Testes funcionais com cenários simulados;
- Testes automatizados com `pytest`;
- Motor temporal com persistência mínima, debounce e TTL;
- Consolidação e deduplicação de identidades;
- Integração implementada com Kafka;
- Adaptador Kafka com controle manual de offsets e retentativas;
- Validação local com mensagens de entrada dos componentes I2, S1 e I9;
- Construção e execução em Docker;
- Configuração Docker Compose validada sintaticamente.

As seguintes validações ainda dependem do ambiente integrado:

- Execução ponta a ponta com o broker Kafka compartilhado;
- Validação com os produtores reais I2, S1 e I9 em funcionamento simultâneo;
- Testes em dispositivo Jetson;
- Testes com câmeras reais.

A implementação do adaptador Kafka e seus testes automatizados não substituem a validação ponta a ponta no ambiente integrado.

## Documentação Complementar

Para compreender os contratos, as interfaces e os detalhes de implementação, consulte:

- `docs/`: documentação arquitetural e técnica;
- `RELATORIO_TESTES_S2.md`: resultados e evidências dos testes;
- `components/S2/regras_s2.exemplo.json`: exemplo de configuração das regras;
- `data/ppe_catalog.exemplo.json`: exemplo de catálogo de EPIs.