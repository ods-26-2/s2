# Relatório de Testes — Componente S2

**Projeto:** ODS  
**Componente:** S2  
**Arquitetura:** 6c  
**Data dos testes:** Novembro de 2026  
**Ambiente:** Linux, Python 3.10/3.11, ambiente virtual Python (`.venv`) e Docker.

## 1. Objetivo

Verificar o funcionamento das principais funcionalidades do componente S2, incluindo consumo e validação de mensagens, criação e associação de entidades globais, fusão multicâmera, processamento de eventos, aplicação de regras temporais, execução dos testes automatizados e funcionamento em contêiner Docker.

## 2. Metodologia

Os testes foram realizados por meio de arquivos JSON contendo mensagens simuladas dos componentes I2, S1 e I9.

A execução local foi realizada utilizando o módulo principal do S2:

```bash
python -m components.S2.main --input <arquivo_entrada.json> --output <arquivo_saida.json>
```

Também foram utilizados scripts Python para gerar cenários específicos e arquivos JSON para representar diferentes situações de processamento.

As entradas e saídas foram inspecionadas por meio de comandos Python e análise dos arquivos JSON produzidos.

Além dos testes manuais, foram executados testes automatizados utilizando `pytest`.

**Origem dos dados:** simulados, não provenientes de câmeras reais.

**Ambiente de execução:** máquina Linux, com ambiente virtual Python. Também foi realizada a execução do componente em contêiner Docker, utilizando a imagem `python:3.11-slim`. Não houve execução em dispositivo Jetson.

## 3. Cenários de teste executados

### 3.1 Testes funcionais e de integração por JSON

| Nº | Cenário | Resultado |
|---|---|---|
| 1 | Criação de entidades globais a partir de mensagens do I2 | Aprovado |
| 2 | Associação de posições do S1 aos rastros do I2 | Aprovado |
| 3 | Manutenção da identidade ao longo de diferentes quadros | Aprovado |
| 4 | Atualização de informações de zona | Aprovado |
| 5 | Fusão multicâmera em condições controladas | Aprovado |
| 6 | Não fusão de entidades com posições distantes | Aprovado |
| 7 | Deduplicação de mensagens por `message_id` | Aprovado |
| 8 | Ciclo de vida do evento `DWELL_EXCEEDED` | Aprovado |
| 9 | Ciclo de vida de `PPE_NON_COMPLIANT` com estado `INCORRECT` | Aprovado |
| 10 | Ciclo de vida de `PPE_NON_COMPLIANT` com estado `ABSENT` | Aprovado |
| 11 | Classificação de confiança como `UNCERTAIN` por confiança intermediária | Aprovado |
| 12 | Não geração de evento abaixo da confiança mínima | Aprovado |
| 13 | Classificação como `UNCERTAIN` com indicador explícito de incerteza | Aprovado |
| 14 | Ciclo de vida dos eventos `ZONE_ENTER` e `ZONE_EXIT` | Aprovado |
| 15 | Validação de mensagens inválidas e campos obrigatórios | Aprovado |
| 16 | Aplicação do debounce em atualizações e reinício de eventos | Aprovado |
| 17 | Associação correta de duas pessoas próximas observadas por duas câmeras | Aprovado |
| 18 | Fusão de observações de três câmeras | Aprovado |
| 19 | Associação de duas pessoas observadas por três câmeras | Aprovado |
| 20 | Respeito ao limite de sincronismo entre câmeras | Aprovado |
| 21 | Não fusão de observações com diferença temporal acima do limite | Aprovado |
| 22 | Não fusão de entidades de espaços diferentes | Aprovado |
| 23 | Não fusão de entidades de classes diferentes | Aprovado |
| 24 | Não fusão de mensagens pertencentes a sessões diferentes | Aprovado |
| 25 | Reidentificação de rastros na mesma câmera dentro do TTL | Aprovado |
| 26 | Criação de nova identidade após expiração do TTL | Aprovado |
| 27 | Processamento de mensagens com timestamps fora de ordem | Aprovado |
| 28 | Processamento de entrada JSON vazia | Aprovado |
| 29 | Tratamento de arquivo JSON malformado | Aprovado |
| 30 | Processamento combinado de múltiplas regras para a mesma pessoa | Aprovado |

### 3.2 Testes automatizados

Foi executado o comando:

```bash
pytest -v
```

O projeto foi configurado por meio do arquivo `pytest.ini`, permitindo executar os testes sem precisar definir manualmente a variável `PYTHONPATH`.

Resultado:

**11 testes aprovados, sem falhas.**

Os testes automatizados verificaram:

- Comportamento do filtro temporal e do debounce;
- Persistência mínima para confirmação de eventos;
- Tratamento de diferentes níveis de confiança;
- Correlação entre mensagens I9 e I2 por `detection_id` e quadro;
- Compatibilidade dos envelopes de entidade e evento com os schemas da arquitetura 6c;
- Encerramento de eventos após o TTL;
- Continuidade da associação multicâmera durante oclusões;
- Emissão das fases `STARTED` e `UPDATED`;
- Reidentificação de rastros;
- Renovação de identidade após expiração do TTL.

## 4. Detalhamento dos resultados

### 4.1 Entidades e rastreamento

Foram verificadas a criação de entidades globais a partir de mensagens do I2, a associação de posições recebidas do S1 e a manutenção dos vínculos entre entidades e rastros ao longo de diferentes quadros.

Também foi verificada a atualização das informações de zona.

Foi confirmado que mensagens isoladas do S1 não criam entidades globais, respeitando a responsabilidade do I2 pela identificação inicial.

### 4.2 Fusão multicâmera

Foram realizados testes com duas e três câmeras, considerando diferentes posições, distâncias, instantes de observação e quantidades de pessoas.

Inicialmente, foi identificada uma falha na associação de pessoas próximas: o algoritmo selecionava o primeiro agrupamento compatível, sem necessariamente comparar todos os candidatos para escolher o mais próximo.

A lógica de agrupamento foi corrigida para selecionar o cluster compatível mais próximo.

Também foi ajustada a reidentificação histórica entre câmeras para respeitar a janela de sincronismo configurada em 100 ms.

Após as correções, foram repetidos os testes com duas pessoas próximas observadas por duas câmeras e com duas pessoas observadas por três câmeras. As associações passaram a ocorrer corretamente.

Também foram verificados:

- Fusão de observações dentro dos limites de distância e sincronismo;
- Não fusão quando a distância ultrapassa o limite configurado;
- Não fusão quando a diferença temporal ultrapassa 100 ms;
- Fusão quando a diferença temporal é exatamente 100 ms;
- Separação de entidades de espaços diferentes;
- Separação de entidades de classes diferentes;
- Separação de entidades pertencentes a sessões diferentes;
- Manutenção da identidade durante o TTL;
- Criação de nova identidade após a expiração do TTL.

**Status:** falha inicial corrigida e cenários repetidos com aprovação.

### 4.3 Processamento de eventos

Foram testados os seguintes tipos de eventos:

- `DWELL_EXCEEDED`;
- `PPE_NON_COMPLIANT`;
- `ZONE_ENTER`;
- `ZONE_EXIT`.

Foram verificadas as fases `STARTED`, `UPDATED` e `ENDED`, incluindo a manutenção do mesmo identificador de evento durante seu ciclo de vida.

No processamento de EPI, foram testados os estados `INCORRECT`, `ABSENT` e `CORRECT`.

Também foram verificadas diferentes condições de confiança:

- Confiança alta, resultando em evento `CONFIRMED`;
- Confiança intermediária, resultando em evento `UNCERTAIN`;
- Confiança abaixo do mínimo configurado, sem geração de evento;
- Indicador explícito de incerteza, resultando em evento `UNCERTAIN`.

Foi realizado um teste combinado de múltiplas regras para a mesma pessoa, verificando o processamento independente dos eventos de EPI e permanência em zona.

### 4.4 Regras temporais

Foram realizados testes com os parâmetros de duração mínima, debounce e TTL.

Foi verificado que uma atualização ocorrida antes do intervalo de debounce não gerou novo evento, enquanto uma atualização após o intervalo configurado foi emitida.

Também foi testado o bloqueio temporário de um novo evento após o encerramento do evento anterior.

O comportamento de expiração e renovação de identidade também foi verificado.

### 4.5 Validação de entradas

Foram testadas mensagens inválidas, incluindo:

- Mensagem I2 sem `track_id`;
- Mensagem S1 com transição inválida;
- Mensagem S1 de saída de zona sem o campo obrigatório `dwell_s`;
- Arquivo JSON malformado;
- Entrada JSON vazia.

O S2 rejeitou as entradas inválidas com mensagens de erro correspondentes e processou corretamente o arquivo vazio.

### 4.6 Execução em Docker

Foi construído o contêiner Docker utilizando a imagem base `python:3.11-slim`.

A imagem foi configurada para instalar as dependências do projeto, copiar os componentes S2 e executar o módulo principal por meio de um `ENTRYPOINT`.

A pasta `data/` foi montada como volume durante a execução, permitindo fornecer arquivos JSON externos e armazenar os resultados diretamente no ambiente local.

Foi executado o cenário combinado de múltiplas regras, com 12 mensagens de entrada.

**Resultado da execução:**

| Indicador | Resultado |
|---|---:|
| Mensagens de entrada | 12 |
| Mensagens de saída | 8 |
| Mensagens `ods.s2.entity` | 4 |
| Mensagens `ods.s2.event` | 4 |

O arquivo de saída foi gerado corretamente em `data/output_s2/`, confirmando a comunicação entre o contêiner e o sistema de arquivos local.

**Status:** construção da imagem, inicialização do contêiner, leitura da entrada, processamento e gravação da saída aprovados.

## 5. Resumo dos resultados

| Indicador | Quantidade |
|---|---:|
| Cenários funcionais e de integração por JSON | 30 |
| Cenários funcionais aprovados | 30 |
| Cenários funcionais com falha pendente | 0 |
| Testes automatizados aprovados | 11 |
| Testes automatizados com falha | 0 |
| Execuções Docker verificadas | 1 |
| Execuções Docker aprovadas | 1 |

A falha inicialmente identificada na associação de pessoas próximas durante a fusão multicâmera foi corrigida e os cenários correspondentes foram executados novamente com sucesso.

Os resultados demonstram o funcionamento das principais funcionalidades implementadas no S2 em cenários controlados.

## 6. Evidências

As evidências dos testes estão organizadas no repositório, incluindo:

- Arquivos JSON utilizados como entrada;
- Scripts Python utilizados para gerar cenários;
- Arquivos JSON produzidos pelo S2;
- Arquivos de saída dos testes de fusão multicâmera;
- Testes automatizados em `tests/`;
- Configuração de testes em `pytest.ini`;
- Configuração Docker em `Dockerfile` e `.dockerignore`;
- Relatório de testes.

**Repositório:** [Inserir link do repositório GitHub]

**Pasta de entradas:** `data/`

**Pasta de saídas:** `data/output_s2/`

**Testes automatizados:** `tests/`

**Configuração Docker:** `Dockerfile` e `.dockerignore`

## 7. Limitações e próximos passos

Apesar dos resultados positivos, os testes realizados ainda se restringem a cenários controlados com mensagens JSON simuladas.

Ainda não foram realizados:

- Integração entre os componentes pelo barramento Kafka;
- Testes com mensagens reais produzidas por I2, S1 e I9;
- Testes ponta a ponta com o fluxo completo em operação;
- Validação em dispositivo Jetson;
- Testes com imagens e câmeras reais;
- Testes de execução contínua do contêiner em um ambiente integrado.

Como próximos passos, estão previstas a implementação da integração Kafka, a validação do fluxo de mensagens entre os componentes e a realização de testes ponta a ponta.

## 8. Conclusão

Os testes demonstraram o funcionamento das principais funcionalidades do componente S2, incluindo criação e manutenção de entidades globais, associação de rastros, fusão multicâmera, processamento de eventos, aplicação de regras temporais e validação de mensagens.

A falha inicialmente identificada na associação de identidades em cenários com pessoas próximas foi corrigida e os testes correspondentes foram aprovados.

Além dos 30 cenários funcionais executados manualmente por meio de JSON, os 11 testes automatizados passaram sem falhas. A execução em Docker também foi validada, incluindo leitura de arquivos externos e gravação dos resultados no ambiente local.

Portanto, o componente S2 apresenta resultados positivos nos testes realizados e possui sua execução local e em contêiner validada. A próxima etapa é a integração com o barramento Kafka e a validação do fluxo completo com os componentes vizinhos.