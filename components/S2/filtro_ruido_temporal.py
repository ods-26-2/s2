"""Primitivas temporais usadas pelo componente S2 (arquitetura 6c).

O debounce é aplicado a uma condição/regra, e não ao fluxo bruto de mensagens:
filtrar quadros inteiros antes da fusão poderia perder observações necessárias
para manter estado, correlacionar câmeras ou encerrar um evento corretamente.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Hashable, Iterable, Mapping


class FiltroRuidoTemporal:
    """Mantém o início das condições e limita repetições por chave lógica."""

    def __init__(self, debounce_window_ms: int = 500) -> None:
        self._validar_inteiro_nao_negativo(
            debounce_window_ms,
            "debounce_window_ms",
        )

        self.debounce_window_ms = debounce_window_ms
        self.ultimo_visto: dict[Hashable, int] = {}
        self.inicio_condicao: dict[Hashable, int] = {}

    @staticmethod
    def _validar_inteiro_nao_negativo(valor: Any, nome: str) -> None:
        """Valida parâmetros que precisam ser inteiros não negativos."""
        if isinstance(valor, bool) or not isinstance(valor, int):
            raise ValueError(f"{nome} deve ser um número inteiro")

        if valor < 0:
            raise ValueError(f"{nome} deve ser maior ou igual a zero")

    @staticmethod
    def _validar_chave(chave: Hashable) -> None:
        """Garante que a chave possa ser utilizada nos dicionários internos."""
        try:
            hash(chave)
        except TypeError as exc:
            raise ValueError("A chave temporal precisa ser hashable") from exc

    @staticmethod
    def timestamp_em_ms(valor: Any) -> int:
        """Converte epoch em ms ou timestamp ISO-8601 com fuso para ms.

        Valores numéricos são interpretados como epoch em milissegundos.
        Strings ISO-8601 precisam informar o fuso horário.
        """
        if isinstance(valor, bool):
            raise ValueError("timestamp booleano não é válido")

        if isinstance(valor, (int, float)):
            if isinstance(valor, float) and not math.isfinite(valor):
                raise ValueError("timestamp numérico precisa ser finito")

            return int(valor)

        if not isinstance(valor, str) or not valor.strip():
            raise ValueError(
                "timestamp deve ser epoch em ms ou string ISO-8601"
            )

        texto = valor.strip()

        # Aceita o sufixo UTC "Z", convertendo-o para o formato
        # de offset reconhecido pelo datetime.fromisoformat().
        if texto.endswith("Z"):
            texto = texto[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(texto)
        except ValueError as exc:
            raise ValueError(
                f"timestamp ISO-8601 inválido: {valor!r}"
            ) from exc

        if dt.tzinfo is None:
            raise ValueError(
                "timestamp ISO-8601 deve incluir fuso horário"
            )

        return int(
            dt.astimezone(timezone.utc).timestamp() * 1000
        )

    def aplicar_debounce(
        self,
        chave: Hashable,
        timestamp_ms: int,
        janela_ms: int | None = None,
    ) -> bool:
        """Retorna True quando a emissão para a chave não viola o debounce.

        A primeira ocorrência é aceita. Ocorrências seguintes só são aceitas
        quando a janela de debounce tiver transcorrido.

        Timestamps anteriores ao último timestamp aceito são rejeitados.
        """
        self._validar_chave(chave)
        self._validar_inteiro_nao_negativo(
            timestamp_ms,
            "timestamp_ms",
        )

        janela = (
            self.debounce_window_ms
            if janela_ms is None
            else janela_ms
        )

        self._validar_inteiro_nao_negativo(
            janela,
            "janela_ms",
        )

        ultimo = self.ultimo_visto.get(chave)

        if ultimo is not None:
            diferenca = timestamp_ms - ultimo

            # Não aceita mensagens fora de ordem.
            if diferenca < 0:
                return False

            # Impede repetições dentro da janela configurada.
            if diferenca < janela:
                return False

        self.ultimo_visto[chave] = timestamp_ms
        return True

    def atualizar_persistencia(
        self,
        chave: Hashable,
        condicao_ativa: bool,
        timestamp_ms: int,
        persistencia_minima_ms: int,
    ) -> tuple[bool, int]:
        """Avalia a persistência contínua de uma condição.

        Retorna:
            (persistiu, duracao_ms)

        Quando a condição fica falsa, seu início é removido para que uma
        ocorrência futura comece um novo ciclo.
        """
        self._validar_chave(chave)
        self._validar_inteiro_nao_negativo(
            timestamp_ms,
            "timestamp_ms",
        )
        self._validar_inteiro_nao_negativo(
            persistencia_minima_ms,
            "persistencia_minima_ms",
        )

        if not isinstance(condicao_ativa, bool):
            raise ValueError("condicao_ativa deve ser bool")

        if not condicao_ativa:
            self.inicio_condicao.pop(chave, None)
            return False, 0

        inicio = self.inicio_condicao.setdefault(
            chave,
            timestamp_ms,
        )

        duracao = max(0, timestamp_ms - inicio)

        return (
            duracao >= persistencia_minima_ms,
            duracao,
        )

    def limpar_sessao(self, session_id: str | None = None) -> None:
        """Limpa o estado de uma sessão.

        Sem session_id, limpa todo o estado temporal.

        Com session_id, remove as chaves em formato de tupla cujo primeiro
        elemento corresponde ao identificador da sessão.
        """
        if session_id is None:
            self.ultimo_visto.clear()
            self.inicio_condicao.clear()
            return

        def pertence_a_sessao(chave: Hashable) -> bool:
            return (
                isinstance(chave, tuple)
                and bool(chave)
                and chave[0] == session_id
            )

        self.ultimo_visto = {
            chave: instante
            for chave, instante in self.ultimo_visto.items()
            if not pertence_a_sessao(chave)
        }

        self.inicio_condicao = {
            chave: instante
            for chave, instante in self.inicio_condicao.items()
            if not pertence_a_sessao(chave)
        }

    def processar(
        self,
        fluxo_eventos: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Filtra eventos de condição legados, sem descartar sinais brutos.

        Este método é mantido como API auxiliar. O pipeline S2 atual deve
        utilizar aplicar_debounce() somente quando decidir emitir uma
        transição de evento.

        Cada item precisa possuir:
        - timestamp_ms, captured_at ou timestamp;
        - uma debounce_key ou campos que formem uma chave lógica estável.
        """
        filtrados: list[dict[str, Any]] = []

        for item in fluxo_eventos:
            evento = dict(item)

            session_id = evento.get("session_id")

            chave = evento.get("debounce_key")

            if chave is not None:
                # Inclui a sessão para evitar colisões entre sessões
                # e permitir a limpeza individual do estado.
                chave = (session_id, chave)

            else:
                rule_id = evento.get("rule_id")
                entity_id = evento.get("entity_id")
                zone_id = evento.get("zone_id")
                equipment_item = evento.get("equipment_item")

                # Não permite que eventos sem identificação lógica
                # compartilhem acidentalmente a mesma chave.
                identificadores = (
                    rule_id,
                    entity_id,
                    zone_id,
                    equipment_item,
                )

                if all(valor is None for valor in identificadores):
                    raise ValueError(
                        "Evento sem debounce_key ou identificadores "
                        "suficientes para formar uma chave lógica"
                    )

                chave = (
                    session_id,
                    rule_id,
                    entity_id,
                    zone_id,
                    equipment_item,
                )

            timestamp = evento.get(
                "timestamp_ms",
                evento.get(
                    "captured_at",
                    evento.get("timestamp"),
                ),
            )

            if timestamp is None:
                raise ValueError(
                    "Evento sem timestamp_ms, captured_at ou timestamp"
                )

            timestamp_ms = self.timestamp_em_ms(timestamp)

            janela = evento.get(
                "debounce_ms",
                self.debounce_window_ms,
            )

            if self.aplicar_debounce(
                chave,
                timestamp_ms,
                janela,
            ):
                filtrados.append(evento)

        return filtrados
