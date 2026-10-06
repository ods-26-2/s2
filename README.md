# Componente S2: Regras Temporais e Eventos

## Descrição

Este repositório contém o código e a documentação do componente S2. A sua responsabilidade principal é processar as inferências brutas, aplicar regras temporais (como limite de taxa/debounce e persistência mínima) e realizar a consolidação e deduplicação de identidades, gerando os eventos canônicos para a próxima camada do sistema.

## Estrutura do Repositório

* **/docs/**: Detalhes arquiteturais e documentação aprofundada estão localizados nesta pasta. **Antes de utilizar este componente, outras equipes devem consultar a documentação disponível aqui.**

## Instruções de Execução (Local)

Nesta fase da Sprint, o componente deve ser configurado e executado no ambiente local.

### Pré-requisitos

* Python 3.x instalado na máquina.

### Como configurar e executar

#### 1. Clone o repositório

Clone o repositório e acesse a pasta raiz do projeto:

```bash
git clone <url-do-repositorio>
cd <nome-do-repositorio>
```

#### 2. Crie e ative um ambiente virtual

É recomendado utilizar um ambiente virtual para isolar as dependências do S2 e evitar conflitos com outras aplicações ou projetos presentes no computador.

**Windows:**

```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**

```bash
python3 -m venv venv
source venv/bin/activate
```

#### 3. Instale as dependências

O componente necessita de pacotes específicos listados no arquivo `requirements.txt`, localizado na raiz do repositório.

Instale as dependências executando:

```bash
pip install -r requirements.txt
```

#### 4. Gere os dados simulados (Mocks)

Como estamos validando a arquitetura em modo *passthrough*, é necessário gerar as coordenadas e inferências falsas que simulam a Camada 2 antes de executar o sistema.

Para gerar os dados simulados, execute:

```bash
python -m components.S2.gerar_mocks
```

#### 5. Execute o componente principal (S2)

Com os dados de teste gerados, inicie a rotina principal para aplicar as regras temporais, realizar o *debounce*, deduplicar as identidades e gerar o Payload Canônico final.

Execute:

```bash
python -m components.S2.main
```

### Fluxo de execução

De forma resumida, a execução local do componente segue o seguinte fluxo:

```text
Clone do repositório
        ↓
Criação do ambiente virtual
        ↓
Instalação das dependências
        ↓
Geração dos dados simulados (Mocks)
        ↓
Execução do componente S2
        ↓
Aplicação das regras temporais
        ↓
Consolidação e deduplicação de identidades
        ↓
Geração dos eventos canônicos
```
