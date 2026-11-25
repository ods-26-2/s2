from __future__ import annotations
from unittest.mock import MagicMock, call, patch

import unittest
from unittest.mock import MagicMock, call

from components.S2.kafka_adapter import (
    AdaptadorKafkaS2,
    _chave_quadro,
    _chave_publicacao,
    _decodificar_mensagem,
)

SESSION = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"


def criar_registro(
    schema: str,
    source: str,
    frame: int,
    offset: int,
    partition: int = 0,
) -> dict:
    mensagem = {
        "message_type": "event",
        "message_id": f"{schema}-{source}-{frame}",
        "schema": schema,
        "schema_version": "1.0",
        "producer": "TEST",
        "published_at": "2026-11-22T15:00:00.000Z",
        "captured_at": "2026-11-22T15:00:00.000Z",
        "session_id": SESSION,
        "source_id": source,
        "sequence": frame,
        "payload": {
            "frame_index": frame,
        },
    }

    return {
        "topic": schema,
        "partition": partition,
        "offset": offset,
        "mensagem": mensagem,
    }


def criar_auxiliar(
    schema: str,
    offset: int,
) -> dict:
    mensagem = {
        "message_type": "event",
        "message_id": f"{schema}-{offset}",
        "schema": schema,
        "schema_version": "1.0",
        "producer": "TEST",
        "published_at": "2026-11-22T15:00:00.000Z",
        "captured_at": "2026-11-22T15:00:00.000Z",
        "session_id": SESSION,
        "sequence": offset,
        "payload": {},
    }

    return {
        "topic": schema,
        "partition": 0,
        "offset": offset,
        "mensagem": mensagem,
    }


class TestAdaptadorKafkaS2(unittest.TestCase):

    def setUp(self):
        self.camada = MagicMock()
        self.camada.validar_mensagem_entrada.side_effect = lambda mensagem: mensagem
        self.adaptador = AdaptadorKafkaS2(self.camada)

    def test_chave_quadro_identifica_sessao_fonte_e_frame(self):
        item = criar_registro(
            "ods.i2.tracks",
            "cam0",
            10,
            0,
        )

        self.assertEqual(
            _chave_quadro(item["mensagem"]),
            (SESSION, "cam0", 10),
        )

    def test_mensagens_do_mesmo_quadro_sao_agrupadas(self):
        i2 = criar_registro("ods.i2.tracks", "cam0", 10, 0)
        s1 = criar_registro("ods.s1.position", "cam0", 10, 1)

        self.adaptador._adicionar_mensagem(i2, 100.0)
        self.adaptador._adicionar_mensagem(s1, 100.01)

        janela = self.adaptador.janelas[SESSION]
        registros = janela["registros"]

        self.assertEqual(len(registros), 2)

        self.assertEqual(
            [registro["chave_quadro"] for registro in registros],
            [
                (SESSION, "cam0", 10),
                (SESSION, "cam0", 10),
            ],
        )

    def test_limite_do_buffer_interrompe_execucao_sem_descartar(self):
        registro1 = criar_registro(
            "ods.i2.tracks", "cam0", 10, 0
        )
        registro2 = criar_registro(
            "ods.i2.tracks", "cam0", 11, 1
        )

        with patch(
            "components.S2.kafka_adapter.KAFKA_BATCH_MAX_MESSAGES",
            1,
        ):
            self.adaptador._adicionar_mensagem(registro1, 100.0)

            with self.assertRaisesRegex(
                RuntimeError, "KAFKA_BATCH_MAX_MESSAGES"
            ):
                self.adaptador._adicionar_mensagem(
                    registro2, 100.1
                )

        # As duas mensagens permanecem armazenadas.
        self.assertEqual(
            len(self.adaptador.janelas[SESSION]["registros"]),
            2,
        )

        self.assertIs(
            self.adaptador.janelas[SESSION]["registros"][0],
            registro1,
        )
        self.assertIs(
            self.adaptador.janelas[SESSION]["registros"][1],
            registro2,
        )

        # Nenhuma mensagem foi processada ou descartada.
        self.camada.processar_mensagens.assert_not_called()

    def test_decodificar_mensagem_rejeita_json_invalido(self):
        with self.assertRaisesRegex(
            ValueError, "JSON UTF-8 válido"
        ):
            _decodificar_mensagem(b"{json invalido")

    def test_decodificar_mensagem_rejeita_conteudo_que_nao_e_objeto(self):
        with self.assertRaisesRegex(
            ValueError, "precisa ser um objeto JSON"
        ):
            _decodificar_mensagem(b'["mensagem", "invalida"]')

    def test_decodificar_mensagem_rejeita_schema_ausente_ou_invalido(self):
        casos = [
            b'{"message_id": "123"}',
            b'{"schema": ""}',
            b'{"schema": 123}',
            b'{"schema": null}',
        ]

        for valor in casos:
            with self.subTest(valor=valor):
                with self.assertRaisesRegex(
                    ValueError, "schema"
                ):
                    _decodificar_mensagem(valor)

    def test_falha_no_commit_preserva_offsets_para_nova_tentativa(self):
        registro = criar_registro(
            "ods.i2.tracks", "cam0", 10, 0
        )

        self.adaptador.consumer = MagicMock()

        # Simula erro retornado pelo Kafka no commit.
        particao_com_erro = MagicMock()
        particao_com_erro.error.return_value = "erro simulado"

        self.adaptador.consumer.commit.side_effect = [
            [particao_com_erro],
            None,
        ]

        self.adaptador._registrar_offset(registro)

        chave_particao = (
            registro["topic"],
            registro["partition"],
        )

        # Primeira tentativa: deve falhar e preservar o offset.
        with self.assertRaisesRegex(
            RuntimeError, "Falha ao confirmar offset Kafka"
        ):
            self.adaptador._marcar_processados([registro])

        self.assertTrue(
            self.adaptador.offsets_pendentes[chave_particao][
                registro["offset"]
            ]
        )

        # Segunda tentativa: deve confirmar e remover o offset.
        self.adaptador._confirmar_offsets()

        self.adaptador.consumer.commit.assert_has_calls(
            [
                call(offsets=unittest.mock.ANY, asynchronous=False),
                call(offsets=unittest.mock.ANY, asynchronous=False),
            ]
        )

        self.assertNotIn(
            chave_particao,
            self.adaptador.offsets_pendentes,
        )

    def test_comando_regras_e_processado_apos_tempo_de_espera(self):
        registro = {
            "topic": "ods.s2.rules.command",
            "partition": 0,
            "offset": 5,
            "mensagem": {
                "schema": "ods.s2.rules.command",
                "message_id": "comando-001",
                "payload": {},
            },
        }

        self.adaptador.tempo_espera = 0.2
        self.adaptador.consumer = MagicMock()
        self.adaptador.producer = MagicMock()

        self.camada.processar_mensagens.return_value = []

        self.adaptador._registrar_offset(registro)
        self.adaptador._adicionar_mensagem(registro, 100.0)

        # Antes do prazo, o comando ainda não deve ser processado.
        self.adaptador._fechar_mensagens_sem_sessao(100.1)
        self.camada.processar_mensagens.assert_not_called()

        # Após o prazo, o comando deve ser encaminhado ao motor.
        self.adaptador._fechar_mensagens_sem_sessao(100.3)

        self.camada.processar_mensagens.assert_called_once_with(
            [registro["mensagem"]]
        )

        self.assertEqual(self.adaptador.sem_sessao, [])
        self.adaptador.consumer.commit.assert_called_once()

    def test_quadros_de_cameras_diferentes_compartilham_janela(self):
        cam0 = criar_registro("ods.i2.tracks", "cam0", 10, 0)
        cam1 = criar_registro("ods.i2.tracks", "cam1", 10, 1)

        self.adaptador._adicionar_mensagem(cam0, 100.0)
        self.adaptador._adicionar_mensagem(cam1, 100.01)

        janela = self.adaptador.janelas[SESSION]
        registros = janela["registros"]

        self.assertEqual(len(registros), 2)

        chaves = {
            registro["chave_quadro"]
            for registro in registros
        }

        self.assertEqual(
            chaves,
            {
                (SESSION, "cam0", 10),
                (SESSION, "cam1", 10),
            },
        )

    def test_comando_regras_e_processado_fora_das_janelas(self):
        registro = {
            "topic": "ods.s2.rules.command",
            "partition": 0,
            "offset": 5,
            "mensagem": {
                "schema": "ods.s2.rules.command",
                "message_id": "comando-001",
                "payload": {},
            },
        }

        self.adaptador._adicionar_mensagem(registro, 100.0)

        # O comando deve ir para o buffer sem sessão.
        self.assertEqual(self.adaptador.janelas, {})
        self.assertEqual(len(self.adaptador.sem_sessao), 1)
        self.assertIs(
            self.adaptador.sem_sessao[0]["registro"],
            registro,
        )

        # O comando não deve ser processado como mensagem de quadro.
        self.camada.processar_mensagens.assert_not_called()

    def test_mensagem_de_quadro_fechado_e_descartada(self):
        registro = criar_registro(
            "ods.i2.tracks", "cam0", 10, 0
        )

        chave = _chave_quadro(registro["mensagem"])

        self.adaptador.consumer = MagicMock()
        self.adaptador._registrar_quadro_fechado(chave)
        self.adaptador._registrar_offset(registro)

        self.adaptador._adicionar_mensagem(registro, 100.0)

        # A mensagem não deve voltar para uma janela de sessão.
        self.assertEqual(self.adaptador.janelas, {})

        # O offset deve ser confirmado imediatamente.
        self.adaptador.consumer.commit.assert_called_once()

        # O registro não deve permanecer pendente.
        chave_particao = (
            registro["topic"],
            registro["partition"],
        )
        self.assertNotIn(
            registro["offset"],
            self.adaptador.offsets_pendentes.get(chave_particao, {}),
        )

        # O motor não deve ser chamado.
        self.camada.processar_mensagens.assert_not_called()

    def test_falha_no_motor_impede_commit_dos_offsets(self):
        registro = criar_registro(
            "ods.i2.tracks", "cam0", 10, 0
        )

        self.adaptador.producer = MagicMock()
        self.adaptador.consumer = MagicMock()

        self.camada.processar_mensagens.side_effect = RuntimeError(
            "falha simulada no motor"
        )

        self.adaptador._registrar_offset(registro)

        with self.assertRaisesRegex(
            RuntimeError, "falha simulada no motor"
        ):
            self.adaptador._processar_registros([registro])

        self.adaptador.producer.produce.assert_not_called()
        self.adaptador.consumer.commit.assert_not_called()

        self.assertFalse(
            self.adaptador.offsets_pendentes[
                (registro["topic"], registro["partition"])
            ][registro["offset"]]
        )

    def test_janela_madura_entrega_registros_com_i2_ao_motor(self):
        cam0_i2 = criar_registro("ods.i2.tracks", "cam0", 10, 0)
        cam0_s1 = criar_registro("ods.s1.position", "cam0", 10, 1)
        cam1_i2 = criar_registro("ods.i2.tracks", "cam1", 10, 2)

        self.adaptador._adicionar_mensagem(cam0_i2, 100.0)
        self.adaptador._adicionar_mensagem(cam0_s1, 100.01)
        self.adaptador._adicionar_mensagem(cam1_i2, 100.02)

        self.adaptador._processar_registros = MagicMock()

        self.adaptador._fechar_janelas_maduras(
            100.0 + self.adaptador.tempo_espera + 1
        )

        self.adaptador._processar_registros.assert_called_once()

        registros = (
            self.adaptador._processar_registros.call_args.args[0]
        )

        self.assertEqual(len(registros), 3)

    def test_auxiliar_da_sessao_e_processada_junto_com_quadro(self):
        i2 = criar_registro("ods.i2.tracks", "cam0", 10, 0)
        auxiliar = criar_auxiliar("ods.session.started", 1)

        self.adaptador._adicionar_mensagem(i2, 100.0)
        self.adaptador._adicionar_mensagem(auxiliar, 100.01)

        self.adaptador._processar_registros = MagicMock()

        self.adaptador._fechar_janelas_maduras(
            100.0 + self.adaptador.tempo_espera + 1
        )

        self.adaptador._processar_registros.assert_called_once()

        registros = (
            self.adaptador._processar_registros.call_args.args[0]
        )

        schemas = [
            registro["mensagem"]["schema"]
            for registro in registros
        ]

        self.assertEqual(
            schemas,
            [
                "ods.i2.tracks",
                "ods.session.started",
            ],
        )

    def test_quadro_sem_i2_e_descartado(self):
        s1 = criar_registro("ods.s1.position", "cam0", 10, 0)

        self.adaptador.consumer = MagicMock()
        self.adaptador._registrar_offset(s1)
        self.adaptador._adicionar_mensagem(s1, 100.0)

        self.adaptador._fechar_janelas_maduras(
            100.0 + self.adaptador.tempo_espera + 1
        )

        self.camada.processar_mensagens.assert_not_called()
        self.assertNotIn(SESSION, self.adaptador.janelas)

    def test_auxiliar_e_preservada_quando_quadro_nao_tem_i2(self):
        s1 = criar_registro("ods.s1.position", "cam0", 10, 0)
        auxiliar = criar_auxiliar("ods.session.started", 1)

        self.adaptador._adicionar_mensagem(s1, 100.0)
        self.adaptador._adicionar_mensagem(auxiliar, 100.01)

        self.adaptador._processar_registros = MagicMock()

        self.adaptador._fechar_janelas_maduras(
            100.0 + self.adaptador.tempo_espera + 1
        )

        self.adaptador._processar_registros.assert_called_once()

        registros = (
            self.adaptador._processar_registros.call_args.args[0]
        )

        self.assertEqual(len(registros), 1)
        self.assertEqual(
            registros[0]["mensagem"]["schema"],
            "ods.session.started",
        )

    def test_motor_e_chamado_por_processar_mensagens(self):
        registro = criar_registro("ods.i2.tracks", "cam0", 10, 0)

        self.adaptador.producer = MagicMock()
        self.adaptador.producer.flush.return_value = 0
        self.adaptador.consumer = MagicMock()

        self.camada.processar_mensagens.return_value = []

        self.adaptador._registrar_offset(registro)
        self.adaptador._processar_registros([registro])

        self.camada.processar_mensagens.assert_called_once_with(
            [registro["mensagem"]]
        )

        self.camada.processar_eventos.assert_not_called()

    def test_publicacao_acontece_antes_da_confirmacao_dos_offsets(self):
        registro = criar_registro("ods.i2.tracks", "cam0", 10, 0)

        saida = {
            "schema": "ods.s2.entity",
            "message_id": "saida-001",
            "session_id": SESSION,
            "payload": {},
        }

        self.adaptador.producer = MagicMock()
        self.adaptador.producer.flush.return_value = 0
        self.adaptador.consumer = MagicMock()

        self.camada.processar_mensagens.return_value = [saida]

        self.adaptador._registrar_offset(registro)

        ordem = MagicMock()
        ordem.attach_mock(self.adaptador.producer, "producer")
        ordem.attach_mock(self.adaptador.consumer, "consumer")

        self.adaptador._processar_registros([registro])

        chamadas = ordem.mock_calls

        chamada_publicacao = call.producer.flush(30.0)
        chamada_commit = call.consumer.commit(
            offsets=unittest.mock.ANY,
            asynchronous=False,
        )

        self.assertLess(
            chamadas.index(chamada_publicacao),
            chamadas.index(chamada_commit),
        )

    def test_offset_posterior_nao_e_confirmado_antes_do_anterior(self):
        self.adaptador.consumer = MagicMock()

        anterior = criar_registro("ods.i2.tracks", "cam0", 10, 10)
        posterior = criar_registro("ods.i2.tracks", "cam0", 11, 11)

        self.adaptador._registrar_offset(anterior)
        self.adaptador._registrar_offset(posterior)

        self.adaptador._marcar_processados([posterior])

        self.adaptador.consumer.commit.assert_not_called()

        self.adaptador._marcar_processados([anterior])

        self.adaptador.consumer.commit.assert_called_once()

        argumentos = self.adaptador.consumer.commit.call_args.kwargs
        offsets = argumentos["offsets"]

        self.assertEqual(len(offsets), 1)
        self.assertEqual(offsets[0].offset, 12)
        self.assertFalse(argumentos["asynchronous"])

    def test_chave_publicacao_prioriza_session_id(self):
        mensagem = {
            "session_id": SESSION,
            "correlation_id": "correlation-1",
            "message_id": "message-1",
        }

        self.assertEqual(
            _chave_publicacao(mensagem),
            SESSION.encode("utf-8"),
        )

    def test_chave_publicacao_usa_correlation_id_sem_sessao(self):
        mensagem = {
            "correlation_id": "correlation-1",
            "message_id": "message-1",
        }

        self.assertEqual(
            _chave_publicacao(mensagem),
            b"correlation-1",
        )

    def test_falha_na_publicacao_impede_commit_dos_offsets(self):
        registro = criar_registro(
            "ods.i2.tracks", "cam0", 10, 0
        )

        saida = {
            "schema": "ods.s2.entity",
            "message_id": "saida-001",
            "session_id": SESSION,
            "payload": {},
        }

        self.adaptador.producer = MagicMock()
        self.adaptador.producer.flush.return_value = 1
        self.adaptador.consumer = MagicMock()

        self.camada.processar_mensagens.return_value = [saida]

        self.adaptador._registrar_offset(registro)

        with self.assertRaises(RuntimeError):
            self.adaptador._processar_registros([registro])

        self.adaptador.consumer.commit.assert_not_called()

        self.assertEqual(
            self.adaptador.offsets_pendentes[
                (registro["topic"], registro["partition"])
            ][registro["offset"]],
            False,
        )


if __name__ == "__main__":
    unittest.main()
