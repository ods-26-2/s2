
"""Adaptador Kafka do componente S2.

Consome mensagens de entrada, agrupa quadros e mensagens auxiliares
por sessão e encaminha os lotes ao motor S2. Publica as saídas e
confirma offsets somente depois da publicação bem-sucedida.
"""

from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

try:
    from .config import (
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_BATCH_MAX_MESSAGES,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_ENABLE_AUTO_COMMIT,
        KAFKA_FRAME_WAIT_MS,
        KAFKA_GROUP_ID,
        KAFKA_INPUT_TOPICS,
        KAFKA_OUTPUT_TOPICS,
        KAFKA_POLL_TIMEOUT_SECONDS,
        SINCRONISMO_FUSAO_MS,
    )
except ImportError:
    from config import (
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_BATCH_MAX_MESSAGES,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_ENABLE_AUTO_COMMIT,
        KAFKA_FRAME_WAIT_MS,
        KAFKA_GROUP_ID,
        KAFKA_INPUT_TOPICS,
        KAFKA_OUTPUT_TOPICS,
        KAFKA_POLL_TIMEOUT_SECONDS,
        SINCRONISMO_FUSAO_MS,
    )


logger = logging.getLogger("S2.kafka")


SCHEMAS_POR_QUADRO = {
    "ods.i2.tracks",
    "ods.i9.ppe_compliance",
    "ods.s1.position",
}

SCHEMA_BASE_QUADRO = "ods.i2.tracks"
SCHEMA_COMANDO_REGRAS = "ods.s2.rules.command"

MAX_QUADROS_FECHADOS_MEMORIA = 10000


def _decodificar_mensagem(valor: bytes) -> dict[str, Any]:
    """Decodifica o valor Kafka como um objeto JSON."""
    try:
        mensagem = json.loads(valor.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Mensagem Kafka não contém JSON UTF-8 válido: {exc}"
        ) from exc

    if not isinstance(mensagem, dict):
        raise ValueError("A mensagem Kafka precisa ser um objeto JSON.")

    schema = mensagem.get("schema")

    if not isinstance(schema, str) or not schema.strip():
        raise ValueError("Mensagem Kafka sem campo 'schema' válido.")

    return mensagem


def _chave_publicacao(mensagem: dict[str, Any]) -> bytes:
    """Gera uma chave Kafka estável para preservar a ordem por sessão."""
    chave = (
        mensagem.get("session_id")
        or mensagem.get("correlation_id")
        or mensagem.get("message_id")
    )

    if not isinstance(chave, str) or not chave.strip():
        raise ValueError(
            "Mensagem de saída sem session_id, correlation_id ou message_id."
        )

    return chave.encode("utf-8")


def _chave_quadro(mensagem: dict[str, Any]) -> tuple[str, str, int]:
    """Extrai a chave de agrupamento de um quadro."""
    session_id = mensagem.get("session_id")
    source_id = mensagem.get("source_id")
    payload = mensagem.get("payload")

    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("Mensagem de quadro sem session_id válido.")

    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("Mensagem de quadro sem source_id válido.")

    if not isinstance(payload, dict):
        raise ValueError("Mensagem de quadro sem payload válido.")

    frame_index = payload.get("frame_index")

    if isinstance(frame_index, bool) or not isinstance(frame_index, int):
        raise ValueError("payload.frame_index precisa ser um inteiro.")

    if frame_index < 0:
        raise ValueError("payload.frame_index não pode ser negativo.")

    return session_id, source_id, frame_index


class AdaptadorKafkaS2:
    """Coordena consumo, buffer, processamento e publicação Kafka."""

    def __init__(self, camada_s2: Any) -> None:
        if camada_s2 is None:
            raise ValueError("É necessário fornecer uma instância de CamadaS2.")

        if not KAFKA_INPUT_TOPICS:
            raise ValueError("Nenhum tópico Kafka de entrada foi configurado.")

        if KAFKA_BATCH_MAX_MESSAGES < 1:
            raise ValueError("KAFKA_BATCH_MAX_MESSAGES precisa ser maior que zero.")

        if KAFKA_FRAME_WAIT_MS < 0:
            raise ValueError("KAFKA_FRAME_WAIT_MS não pode ser negativo.")

        if KAFKA_POLL_TIMEOUT_SECONDS <= 0:
            raise ValueError("KAFKA_POLL_TIMEOUT_SECONDS precisa ser positivo.")

        self.camada_s2 = camada_s2
        self.tempo_espera = KAFKA_FRAME_WAIT_MS / 1000.0

        self.consumer: Consumer | None = None
        self.producer: Producer | None = None

        # session_id -> {
        #     "inicio": instante_monotônico,
        #     "registros": [registros da sessão]
        # }
        #
        # Quadros e mensagens auxiliares da mesma sessão ficam juntos.
        self.janelas: dict[str, dict[str, Any]] = {}

        # Mensagens sem sessão e comandos de regras.
        # São processados em uma janela independente.
        self.sem_sessao: list[dict[str, Any]] = []

        # (topic, partition) -> {offset: processado}.
        self.offsets_pendentes: dict[
            tuple[str, int], dict[int, bool]
        ] = {}

        # Chaves de quadros já encerrados, para detectar mensagens atrasadas.
        self.quadros_fechados: OrderedDict[
            tuple[str, str, int], None
        ] = OrderedDict()

    def _criar_clientes(self) -> None:
        """Cria os clientes Kafka sem iniciar processamento."""
        configuracao_consumer = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
            "enable.auto.commit": KAFKA_ENABLE_AUTO_COMMIT,
            "enable.auto.offset.store": False,
        }

        configuracao_producer = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }

        self.consumer = Consumer(configuracao_consumer)
        self.producer = Producer(configuracao_producer)

        self.consumer.subscribe(KAFKA_INPUT_TOPICS)

        logger.info(
            "Consumidor S2 inscrito nos tópicos: %s",
            ", ".join(KAFKA_INPUT_TOPICS),
        )

    def _registrar_offset(self, registro: dict[str, Any]) -> None:
        """Registra um offset recebido, ainda não processado."""
        chave = (registro["topic"], registro["partition"])

        self.offsets_pendentes.setdefault(chave, {})[
            registro["offset"]
        ] = False

    def _marcar_processados(self, registros: list[dict[str, Any]]) -> None:
        """Marca registros concluídos e tenta confirmar offsets seguros."""
        for registro in registros:
            chave = (registro["topic"], registro["partition"])
            offset = registro["offset"]

            pendentes = self.offsets_pendentes.get(chave)

            if pendentes is not None and offset in pendentes:
                pendentes[offset] = True

        self._confirmar_offsets()

    def _confirmar_offsets(self) -> None:
        """Confirma somente sequências contíguas de registros processados."""
        if self.consumer is None:
            return

        offsets_commit: list[TopicPartition] = []
        avancos: dict[tuple[str, int], int] = {}

        for chave, pendentes in self.offsets_pendentes.items():
            if not pendentes:
                continue

            offsets_ordenados = sorted(pendentes)
            primeiro_pendente = None

            for offset in offsets_ordenados:
                if not pendentes[offset]:
                    primeiro_pendente = offset
                    break

            if primeiro_pendente is not None:
                proximo_offset = primeiro_pendente
            else:
                proximo_offset = offsets_ordenados[-1] + 1

            menor_offset = offsets_ordenados[0]

            if proximo_offset <= menor_offset:
                continue

            topic, partition = chave

            offsets_commit.append(
                TopicPartition(topic, partition, proximo_offset)
            )
            avancos[chave] = proximo_offset

        if not offsets_commit:
            return

        resultado = self.consumer.commit(
            offsets=offsets_commit,
            asynchronous=False,
        )

        if resultado:
            for particao in resultado:
                erro = getattr(particao, "error", None)

                if callable(erro):
                    erro = erro()

                if erro:
                    raise RuntimeError(
                        f"Falha ao confirmar offset Kafka: {erro}"
                    )

        for chave, proximo_offset in avancos.items():
            pendentes = self.offsets_pendentes[chave]

            for offset in list(pendentes):
                if offset < proximo_offset:
                    del pendentes[offset]

            if not pendentes:
                del self.offsets_pendentes[chave]

        logger.debug("Offsets Kafka confirmados: %s", offsets_commit)

    def _publicar_saidas(self, saidas: list[dict[str, Any]]) -> None:
        """Publica as saídas do motor e aguarda as confirmações do broker."""
        if not saidas:
            return

        if self.producer is None:
            raise RuntimeError("Produtor Kafka não foi inicializado.")

        erros_entrega: list[str] = []

        def callback_entrega(erro: Any, mensagem: Any) -> None:
            if erro is not None:
                erros_entrega.append(str(erro))

        for saida in saidas:
            if not isinstance(saida, dict):
                raise ValueError("O motor S2 produziu uma saída inválida.")

            schema = saida.get("schema")
            topico = KAFKA_OUTPUT_TOPICS.get(schema)

            if topico is None:
                raise ValueError(
                    f"Não existe tópico Kafka configurado para o schema {schema!r}."
                )

            chave = _chave_publicacao(saida)

            valor = json.dumps(
                saida,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")

            while True:
                try:
                    self.producer.produce(
                        topic=topico,
                        key=chave,
                        value=valor,
                        on_delivery=callback_entrega,
                    )
                    break
                except BufferError:
                    self.producer.poll(1.0)

        mensagens_pendentes = self.producer.flush(30.0)

        if mensagens_pendentes:
            raise RuntimeError(
                f"{mensagens_pendentes} mensagem(ns) de saída "
                "não foram confirmadas pelo Kafka."
            )

        if erros_entrega:
            raise RuntimeError(
                "Falha na publicação de saídas Kafka: "
                + "; ".join(erros_entrega)
            )

        logger.info("%s mensagem(ns) de saída publicadas.", len(saidas))

    def _processar_registros(self, registros: list[dict[str, Any]]) -> None:
        """Executa o motor uma vez e publica todas as suas saídas."""
        if not registros:
            return

        mensagens = [registro["mensagem"] for registro in registros]

        # Usa a API existente do motor S2.
        # Se houver falha, não repetimos o lote nesta instância:
        # o motor pode ter alterado parcialmente seu estado em memória.
        saidas = self.camada_s2.processar_mensagens(mensagens)

        self._publicar_saidas(saidas)

        # Os offsets só são confirmados depois da publicação bem-sucedida.
        self._marcar_processados(registros)

        logger.info(
            "Lote processado: %s mensagem(ns) de entrada e %s saída(s).",
            len(registros),
            len(saidas),
        )

    def _registrar_quadro_fechado(
        self,
        chave: tuple[str, str, int],
    ) -> None:
        """Armazena a chave de um quadro encerrado, com limite de memória."""
        self.quadros_fechados[chave] = None
        self.quadros_fechados.move_to_end(chave)

        while len(self.quadros_fechados) > MAX_QUADROS_FECHADOS_MEMORIA:
            self.quadros_fechados.popitem(last=False)

    def _total_bufferizado(self) -> int:
        """Retorna a quantidade de registros atualmente em buffer."""
        total_sessoes = sum(
            len(janela["registros"])
            for janela in self.janelas.values()
        )

        return total_sessoes + len(self.sem_sessao)

    def _adicionar_mensagem(
        self,
        registro: dict[str, Any],
        agora: float,
    ) -> None:
        """Direciona a mensagem para a janela adequada."""
        mensagem = registro["mensagem"]

        try:
            mensagem = self.camada_s2.validar_mensagem_entrada(mensagem)
        except ValueError as exc:
            logger.warning(
                "Mensagem Kafka inválida descartada: topic=%s "
                "partition=%s offset=%s erro=%s",
                registro["topic"],
                registro["partition"],
                registro["offset"],
                exc,
            )
            self._marcar_processados([registro])
            return

        registro["mensagem"] = mensagem
        schema = mensagem["schema"]

        # Comandos de regras são tratados separadamente das sessões.
        if schema == SCHEMA_COMANDO_REGRAS:
            self.sem_sessao.append(
                {
                    "inicio": agora,
                    "registro": registro,
                }
            )

        elif schema in SCHEMAS_POR_QUADRO:
            try:
                chave = _chave_quadro(mensagem)
            except ValueError as exc:
                logger.warning(
                    "Mensagem sem chave de quadro válida descartada: "
                    "schema=%s topic=%s partition=%s offset=%s erro=%s",
                    schema,
                    registro["topic"],
                    registro["partition"],
                    registro["offset"],
                    exc,
                )
                self._marcar_processados([registro])
                return

            if chave in self.quadros_fechados:
                logger.warning(
                    "Mensagem atrasada descartada: schema=%s session=%s "
                    "source=%s frame=%s",
                    schema,
                    chave[0],
                    chave[1],
                    chave[2],
                )
                self._marcar_processados([registro])
                return

            session_id = chave[0]

            janela = self.janelas.setdefault(
                session_id,
                {
                    "inicio": agora,
                    "registros": [],
                },
            )

            registro["chave_quadro"] = chave
            janela["registros"].append(registro)

        else:
            session_id = mensagem.get("session_id")

            if isinstance(session_id, str) and session_id.strip():
                # Mensagens auxiliares da sessão ficam na mesma janela
                # dos quadros, preservando a linha temporal do motor.
                janela = self.janelas.setdefault(
                    session_id,
                    {
                        "inicio": agora,
                        "registros": [],
                    },
                )

                janela["registros"].append(registro)

            else:
                # Mensagens sem sessão são processadas separadamente.
                self.sem_sessao.append(
                    {
                        "inicio": agora,
                        "registro": registro,
                    }
                )

        total_bufferizado = self._total_bufferizado()

        if total_bufferizado > KAFKA_BATCH_MAX_MESSAGES:
            raise RuntimeError(
                "O buffer Kafka ultrapassou KAFKA_BATCH_MAX_MESSAGES. "
                "A execução será interrompida sem descartar mensagens "
                "ou fechar quadros prematuramente."
            )

    def _fechar_janelas_maduras(self, agora: float) -> None:
        """Processa janelas de sessão que ultrapassaram a tolerância."""
        sessoes_maduras = [
            session_id
            for session_id, janela in self.janelas.items()
            if agora - janela["inicio"] >= self.tempo_espera
        ]

        for session_id in sessoes_maduras:
            janela = self.janelas.pop(session_id)

            registros_por_quadro: dict[
                tuple[str, str, int], list[dict[str, Any]]
            ] = {}

            registros_auxiliares: list[dict[str, Any]] = []

            for registro in janela["registros"]:
                chave = registro.get("chave_quadro")

                if chave is None:
                    registros_auxiliares.append(registro)
                else:
                    registros_por_quadro.setdefault(chave, []).append(
                        registro
                    )

            registros_prontos: list[dict[str, Any]] = []
            registros_descartados: list[dict[str, Any]] = []

            for chave, registros in registros_por_quadro.items():
                self._registrar_quadro_fechado(chave)

                possui_i2 = any(
                    registro["mensagem"].get("schema") == SCHEMA_BASE_QUADRO
                    for registro in registros
                )

                if possui_i2:
                    registros_prontos.extend(registros)
                else:
                    registros_descartados.extend(registros)

                    logger.warning(
                        "Quadro descartado por ausência de I2: "
                        "session=%s source=%s frame=%s",
                        chave[0],
                        chave[1],
                        chave[2],
                    )

            # Quadros sem I2 não geram entidade no motor.
            # Seus offsets podem ser confirmados após o descarte registrado.
            if registros_descartados:
                self._marcar_processados(registros_descartados)

            # Mensagens auxiliares são preservadas mesmo quando não há
            # quadros válidos. Assim, eventos de sessão e zona não somem.
            registros_prontos.extend(registros_auxiliares)

            if registros_prontos:
                # Quadros válidos e auxiliares da sessão são enviados juntos.
                # O motor organiza os eventos pela linha temporal.
                self._processar_registros(registros_prontos)

    def _fechar_mensagens_sem_sessao(self, agora: float) -> None:
        """Processa comandos e demais mensagens sem sessão após a espera."""
        prontas = [
            item["registro"]
            for item in self.sem_sessao
            if agora - item["inicio"] >= self.tempo_espera
        ]

        if not prontas:
            return

        ids_prontos = {id(registro) for registro in prontas}

        self.sem_sessao = [
            item
            for item in self.sem_sessao
            if id(item["registro"]) not in ids_prontos
        ]

        self._processar_registros(prontas)

    def _fechar_buffers_maduros(self) -> None:
        """Fecha todas as janelas cujo tempo de espera terminou."""
        agora = time.monotonic()

        self._fechar_janelas_maduras(agora)
        self._fechar_mensagens_sem_sessao(agora)

    def executar(self) -> None:
        """Inicia o consumo contínuo do Kafka."""
        self._criar_clientes()

        if self.consumer is None:
            raise RuntimeError("Consumidor Kafka não foi inicializado.")

        logger.info(
            "Adaptador Kafka S2 iniciado. Espera por quadro: %s ms; "
            "sincronismo do motor: %s ms.",
            KAFKA_FRAME_WAIT_MS,
            SINCRONISMO_FUSAO_MS,
        )

        try:
            while True:
                # Fecha primeiro as janelas vencidas. Assim, uma mensagem
                # recebida depois do prazo não entra silenciosamente no lote.
                self._fechar_buffers_maduros()

                mensagem_kafka = self.consumer.poll(
                    KAFKA_POLL_TIMEOUT_SECONDS
                )

                if mensagem_kafka is None:
                    continue

                erro = mensagem_kafka.error()

                if erro is not None:
                    if erro.code() == KafkaError._PARTITION_EOF:
                        continue

                    raise RuntimeError(f"Erro no consumo Kafka: {erro}")

                registro = {
                    "topic": mensagem_kafka.topic(),
                    "partition": mensagem_kafka.partition(),
                    "offset": mensagem_kafka.offset(),
                    "mensagem": None,
                }

                self._registrar_offset(registro)

                valor = mensagem_kafka.value()

                try:
                    if valor is None:
                        raise ValueError(
                            "Mensagem Kafka recebida sem conteúdo."
                        )

                    registro["mensagem"] = _decodificar_mensagem(valor)

                except ValueError as exc:
                    logger.warning(
                        "Registro Kafka inválido descartado: topic=%s "
                        "partition=%s offset=%s erro=%s",
                        registro["topic"],
                        registro["partition"],
                        registro["offset"],
                        exc,
                    )

                    self._marcar_processados([registro])
                    continue

                self._adicionar_mensagem(
                    registro,
                    time.monotonic(),
                )

        except KeyboardInterrupt:
            logger.info(
                "Interrupção solicitada. Mensagens ainda no buffer "
                "permanecerão sem confirmação de offset."
            )

        finally:
            if self.producer is not None:
                pendentes = self.producer.flush(10.0)

                if pendentes:
                    logger.error(
                        "%s mensagem(ns) ainda pendentes no produtor.",
                        pendentes,
                    )

            if self.consumer is not None:
                self.consumer.close()

            logger.info("Adaptador Kafka S2 encerrado.")


def executar_kafka(camada_s2: Any) -> None:
    """Ponto de entrada utilizado pelo main.py."""
    adaptador = AdaptadorKafkaS2(camada_s2)
    adaptador.executar()
