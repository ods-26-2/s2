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
- Classificação de eventos conforme níveis de confiança e incerteza.

## Estrutura do Repositório

- `components/S2/`: implementação do componente S2, incluindo motor de gerenciamento de estado, filtro temporal, configurações e regras demonstrativas.
- `tests/`: testes automatizados do componente.
- `data/`: arquivos JSON utilizados como entradas e saídas nos testes.
- `data/output_s2/`: arquivos JSON produzidos pelo componente.
- `docs/`: documentação arquitetural e documentação complementar.
- `Dockerfile`: configuração para construção da imagem Docker.
- `dockerignore`: configuração dos arquivos excluídos do contexto de construção da imagem.
- `pytest.ini`: configuração dos testes automatizados.
- `requirements.txt`: dependências do projeto.
- `RELATORIO_TESTES_S2.md`: relatório dos testes realizados.

**Antes de utilizar este componente, as equipes responsáveis pela integração devem consultar a documentação disponível em `docs/` e os contratos de mensagens da arquitetura 6c.**

## Instruções de Execução Local

Nesta etapa, o componente pode ser configurado e executado localmente por meio de arquivos JSON.

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
python -m components.S2.main --input data/entrada_teste_multiplas_regras.json --output data/output_s2/saida_s2.json
```

### 5. Configuração das regras

O componente utiliza um artefato RULES para determinar as condições de geração dos eventos.

É possível informar um arquivo de regras explicitamente:

```bash
python -m components.S2.main --input data/entrada_teste_multiplas_regras.json --output data/output_s2/saida_s2.json --rules data/regras_s2.json
```

Quando o argumento `--rules` não é informado, o componente procura primeiro `data/regras_s2.json` e, caso não exista, utiliza `components/S2/regras_s2.exemplo.json`, se disponível.

O arquivo de regras demonstrativas serve para validação e testes. Para utilização integrada, deve ser fornecido o artefato RULES correspondente ao ambiente.

### Argumentos disponíveis

| Argumento | Descrição |
|---|---|
| `--input` ou `-i` | Caminho do JSON de entrada |
| `--output` ou `-o` | Caminho do JSON de saída |
| `--rules` ou `-r` | Caminho do artefato de regras |
| `--fusao-mock` | Utiliza a entrada demonstrativa de fusão quando a entrada padrão está selecionada |

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
- Expiração e renovação de identidades.

**Última validação registrada:** 11 testes aprovados.

Para mais detalhes, consulte `RELATORIO_TESTES_S2.md`.

## Execução com Docker

O projeto também permite executar o componente em um contêiner Docker.

### 1. Pré-requisitos

- Docker instalado e em execução.

### 2. Construa a imagem

Na raiz do repositório, execute:

```bash
docker build -t ods-s2 .
```

### 3. Execute o componente

A pasta `data/` deve ser montada como volume para permitir que o contêiner leia os arquivos de entrada e grave os resultados no ambiente local.

Exemplo:

```bash
docker run --rm -v "$(pwd)/data:/app/data" ods-s2 --input /app/data/entrada_teste_multiplas_regras.json --output /app/data/output_s2/saida_docker.json
```

No Linux, esse comando compartilha a pasta `data/` do projeto com o contêiner.

Assim, os arquivos de entrada podem ser alterados sem reconstruir a imagem, e os resultados gerados ficam disponíveis em `data/output_s2/`.

No Windows PowerShell, utilize:

```powershell
docker run --rm -v "${PWD}/data:/app/data" ods-s2 --input /app/data/entrada_teste_multiplas_regras.json --output /app/data/output_s2/saida_docker.json
```

## Fluxo de Execução

O fluxo atual de execução local pode ser resumido da seguinte forma:

```text
Clone do repositório
        ↓
Criação do ambiente virtual
        ↓
Instalação das dependências
        ↓
Preparação do arquivo JSON de entrada
        ↓
Carregamento das regras
        ↓
Execução do componente S2
        ↓
Validação e processamento das mensagens
        ↓
Consolidação e deduplicação de identidades
        ↓
Aplicação das regras temporais
        ↓
Geração das entidades e eventos
        ↓
Gravação do JSON de saída
```

## Estado Atual da Integração

O componente já possui:

- Processamento local por arquivos JSON;
- Testes funcionais com cenários simulados;
- Testes automatizados com `pytest`;
- Construção e execução em Docker.

Ainda estão pendentes:

- Integração com o barramento Kafka;
- Validação com mensagens reais produzidas pelos componentes I2, S1 e I9;
- Testes ponta a ponta com o fluxo integrado;
- Validação em dispositivo Jetson;
- Testes com câmeras reais.

A integração Kafka será realizada em uma etapa posterior, após a validação dos fluxos por JSON.

## Documentação Complementar

Para compreender os contratos, as interfaces e os detalhes de implementação, consulte:

- `docs/`: documentação arquitetural e técnica;
- `RELATORIO_TESTES_S2.md`: resultados e evidências dos testes;
- `components/S2/regras_s2.exemplo.json`: exemplo de configuração das regras.