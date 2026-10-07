# Documentação Arquitetural — S2: Regras Temporais e Eventos

## 1. Visão Geral do Componente

O componente **S2 (Regras Temporais e Eventos)** atua como o motor de purificação e consolidação de dados na **Camada 3 (Serviços)** do sistema. O seu propósito exclusivo é receber inferências brutas e ruidosas da Camada 2 (juntamente com os dados espaciais do componente S1), estabilizá-las temporalmente e unificá-las.

O S2 atua como um escudo protetor para a **Camada 4 (Aplicação/A3)**: ele garante que o motor de decisão receba apenas eventos consistentes, eliminando flutuações de detecção, duplicações de câmera e falsos positivos de borda.

---

## 2. Princípios de Design Adotados

O desenvolvimento do S2 foi pautado em dois princípios fundamentais da Engenharia de Software:

### 2.1. Separação de Interesses (*Separation of Concerns*)

O S2 não toma decisões de negócio nem emite alertas. A sua responsabilidade limita-se à estabilização técnica dos dados (limpeza e deduplicação).

A semântica do que é um **"risco crítico"** ou **"situação normal"** é delegada inteiramente ao componente **A3**.

### 2.2. Ocultação de Informação (*Information Hiding*)

Os parâmetros de configuração e as estruturas de estado utilizadas pelo S2 são mantidos como atributos das classes responsáveis pelo processamento. Dessa forma, o gerenciamento desses valores fica concentrado nos componentes que implementam as respectivas regras de processamento.

---

## 3. Funcionalidades e Regras de Processamento

### 3.1. Filtro de Ruído Temporal (Janela e Debounce)

Detecções visuais podem apresentar oscilações rápidas, especialmente em situações próximas aos limites de uma zona de interesse.

Para reduzir o impacto de eventos repetidos em um curto intervalo, o S2 aplica um mecanismo de debounce temporal.

### 3.2. Estado em Memória e Deduplicação

O S2 mantém informações das entidades processadas em memória por meio do atributo
`estado_em_memoria`, utilizado pelo `MotorGerenciamentoEstadoConcorrente`.

Esse estado auxilia o processamento das entidades durante a consolidação e
deduplicação dos eventos recebidos. A fusão entre eventos considera a proximidade
temporal e espacial das detecções, utilizando os parâmetros
`time_sync_threshold_ms` e `merge_distance_threshold`.

O código atual ainda não implementa um mecanismo de TTL para expiração automática das
entidades nem um rastreador específico para persistência durante o desaparecimento
temporário de uma entidade.

### 3.3. Fusão e Deduplicação de Identidades

O `MotorGerenciamentoEstadoConcorrente` resolve conflitos oriundos de múltiplas fontes (várias câmeras apontando para o mesmo local).

Utilizando limiares de distância espacial e sincronia temporal, o sistema correlaciona detecções concorrentes e funde as identidades, emitindo um único `global_entity_id`.

### 3.4. Governança de Confiança e Dúvida Explícita

O S2 atua como o primeiro filtro de incerteza da IA, roteando os eventos com base no score de confiança recebido:

- **Score Alto (`>= 0.8`):** Aprovado e consolidado automaticamente.
- **Score Baixo (`< 0.5`):** Descarte sumário (*drop*) para evitar lixo no sistema.
- **Dúvida Explícita (`0.5 a 0.8`):** A entidade é repassada no Payload Canônico com o status de **"dúvida explícita"**, forçando o motor de regras da Camada 4 a solicitar revisão e intervenção de um Operador Humano.

---

## 4. Interface de Saída: O Evento Canônico

Após passar por todos os filtros e pelo motor de estado, o S2 constrói o **Evento Canônico**.

Este DTO (*Data Transfer Object*) possui todos os seus atributos expostos publicamente para facilitar o consumo da Camada 4.

O contrato estabelecido garante a entrega das seguintes informações estruturadas em JSON:

- **ID Global Unificado:** `global_entity_id`
- **Fontes Originárias:** `fontes`
- **Pontuação de Confiança Consolidada:** `pontuacao_confianca`
- **Status de Processamento:** `status_final`
- **Timestamp da consolidação**

---

## 5. Estratégia de Simulação e Testes (Passthrough)

Durante as etapas de validação arquitetural (Sprints iniciais), o S2 é testado em modo isolado através de uma estratégia de *mocking*.

O script `gerar_mocks.py` simula as camadas anteriores, gerando o arquivo `s1_evento_espacial`, que imita a entrada de dados crus com ruídos e duplicações intencionais.

Ao processar este arquivo, o S2 aplica efetivamente suas lógicas de filtro e gera a saída `fusao_deduplicacao`.

Essa abordagem prova o funcionamento das regras temporais de ponta a ponta sem a necessidade de hardware físico ou de integração prematura com os algoritmos de visão computacional.
