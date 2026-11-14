# Relatório de Testes — Componente S2

**Projeto:** ODS  
**Componente:** S2  
**Arquitetura:** 6c  
**Data dos testes:** Novembro de 2026  
**Ambiente:** Linux, ambiente virtual Python (`.venv`)

## 1. Objetivo

Verificar o funcionamento das principais funcionalidades do componente S2, incluindo consumo e validação de mensagens, criação e associação de entidades globais, fusão multicâmera, processamento de eventos e aplicação de regras temporais.

## 2. Metodologia

Os testes foram realizados manualmente por meio de arquivos JSON contendo mensagens simuladas dos componentes I2, S1 e I9.

A execução foi feita utilizando o módulo principal do S2:

```bash
python -m components.S2.main --input <arquivo_entrada.json> --output <arquivo_saida.json>
```

Também foram utilizados scripts Python para gerar cenários específicos.

As entradas e saídas foram inspecionadas por meio de comandos Python e análise dos arquivos JSON produzidos.

**Origem dos dados:** simulados, não provenientes de câmeras reais.

**Ambiente de execução:** máquina Linux do laboratório, em ambiente virtual Python. Não houve execução em Jetson.

## 3. Cenários de teste executados

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
| 17 | Associação de duas pessoas próximas observadas por duas câmeras | Falhou |

## 4. Detalhamento dos resultados

### 4.1 Entidades e rastreamento

Foram verificadas a criação de entidades globais a partir de mensagens do I2, a associação de posições recebidas do S1 e a manutenção dos vínculos entre entidades e rastros ao longo de diferentes quadros.

Também foi verificada a atualização das informações de zona.

### 4.2 Fusão multicâmera

Foi realizado um teste com duas câmeras, nas quais observações correspondentes foram posicionadas próximas umas das outras, no mesmo espaço e instante.

O S2 consolidou corretamente os rastros em uma entidade global.

Também foi realizado um cenário com posições suficientemente distantes, no qual o S2 manteve duas entidades separadas.

Entretanto, no teste com duas pessoas próximas, cada uma observada por duas câmeras, ocorreu uma associação incorreta entre os rastros.

O resultado apresentou duas entidades globais, mas os rastros das câmeras foram associados às pessoas erradas.

A análise do código identificou que a formação dos agrupamentos seleciona o primeiro cluster compatível, sem necessariamente comparar todos os candidatos para escolher o mais próximo.

**Status:** falha identificada; correção pendente.

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

### 4.4 Regras temporais

Foram realizados testes com os parâmetros de duração mínima e debounce.

Foi verificado que uma atualização ocorrida antes do intervalo de debounce não gerou novo evento, enquanto uma atualização após o intervalo configurado foi emitida.

Também foi testado o bloqueio temporário de um novo evento após o encerramento do evento anterior.

### 4.5 Validação de entradas

Foram testadas mensagens inválidas, incluindo:

- Mensagem I2 sem `track_id`;
- Mensagem S1 com transição inválida;
- Mensagem S1 de saída de zona sem o campo obrigatório `dwell_s`.

O S2 rejeitou as entradas inválidas e apresentou mensagens de erro correspondentes.

## 5. Resumo dos resultados

| Indicador | Quantidade |
|---|---:|
| Cenários executados | 17 |
| Cenários aprovados | 16 |
| Cenários com falha | 1 |

A falha encontrada está relacionada à associação de identidades durante a fusão multicâmera de pessoas próximas.

A falha foi reproduzida, analisada no código-fonte e documentada. Sua correção ainda não foi implementada.

## 6. Evidências

As evidências dos testes devem ser disponibilizadas no repositório, incluindo:

- Arquivos JSON utilizados como entrada;
- Scripts Python utilizados para gerar cenários;
- Arquivos JSON produzidos pelo S2;
- Resultados dos testes de fusão multicâmera;
- Registro do cenário que apresentou associação incorreta.

**Repositório:** [Inserir link do repositório GitHub]

**Pasta de entradas:** `data/`

**Pasta de saídas:** `data/output_s2/`

**Scripts de geração:** `components/S2/` e arquivos geradores utilizados nos testes.

## 7. Limitações e próximos passos

Os testes realizados validam funcionalidades do S2 em cenários controlados com mensagens JSON simuladas.

Ainda não foram realizados:

- Integração entre os componentes pelo barramento Kafka;
- Testes com mensagens reais produzidas por I2, S1 e I9;
- Testes ponta a ponta com o fluxo completo em operação;
- Validação em dispositivo Jetson;
- Testes com imagens e câmeras reais.

Como próximo passo, está prevista a correção da lógica de associação multicâmera, seguida da repetição do cenário com duas pessoas próximas e da ampliação dos testes de integração.

## 8. Conclusão

Os testes demonstraram o funcionamento de diversas funcionalidades centrais do componente S2, incluindo criação de entidades, associação de rastros, processamento de eventos, regras temporais e validação de mensagens.

Foi identificada uma limitação na associação de identidades durante a fusão multicâmera em cenários com pessoas próximas. O problema foi documentado e permanece pendente de correção.

Portanto, o componente apresentou resultados positivos nos testes funcionais realizados, mas ainda necessita de correção e validação por meio da integração com os componentes vizinhos e do barramento de comunicação.