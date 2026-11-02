"""Primitivas temporais usadas pelo componente S2 (arquitetura 6c).

O debounce é aplicado a uma condição/regra, e não ao fluxo bruto de mensagens:
filtrar quadros inteiros antes da fusão poderia perder observações necessárias
para manter estado, correlacionar câmeras ou encerrar um evento corretamente.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Hashable, Iterable, Mapping


class FiltroRuidoTemporal:
    """Mantém o início das condições e limita repetições por chave lógica."""

    def __init__(self, debounce_window_ms: int = 500) -> None:
        if debounce_window_ms < 0:
            raise ValueError("debounce_window_ms deve ser maior ou igual a zero")
        self.debounce_window_ms = debounce_window_ms
        self.ultimo_visto: dict[Hashable, int] = {}
        self.inicio_condicao: dict[Hashable, int] = {}

    @staticmethod
    def timestamp_em_ms(valor: Any) -> int:
        """Converte epoch em ms ou timestamp ISO-8601 UTC para milissegundos."""
        if isinstance(valor, bool):
            raise ValueError("timestamp booleano não é válido")
        if isinstance(valor, (int, float)):
            return int(valor)
        if not isinstance(valor, str) or not valor.strip():
            raise ValueError("timestamp deve ser epoch em ms ou string ISO-8601")
        texto = valor.strip()
        try:
            dt = datetime.fromisoformat(texto.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"timestamp ISO-8601 inválido: {valor!r}") from exc
        if dt.tzinfo is None:
            raise ValueError("timestamp ISO-8601 deve incluir fuso horário")
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)

    def aplicar_debounce(
        self,
        chave: Hashable,
        timestamp_ms: int,
        janela_ms: int | None = None,
    ) -> bool:
        """Retorna True quando a emissão para *chave* não viola o debounce."""
        janela = self.debounce_window_ms if janela_ms is None else janela_ms
        if janela < 0:
            raise ValueError("janela de debounce deve ser maior ou igual a zero")
        timestamp_ms = int(timestamp_ms)
        ultimo = self.ultimo_visto.get(chave)
        if ultimo is not None and timestamp_ms - ultimo < janela:
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

        Retorna (persistiu, duração_ms). Ao observar a condição falsa, limpa seu
        início para que uma futura ocorrência comece um ciclo novo.
        """
        if persistencia_minima_ms < 0:
            raise ValueError("persistencia_minima_ms deve ser maior ou igual a zero")
        timestamp_ms = int(timestamp_ms)
        if not condicao_ativa:
            self.inicio_condicao.pop(chave, None)
            return False, 0
        inicio = self.inicio_condicao.setdefault(chave, timestamp_ms)
        duracao = max(0, timestamp_ms - inicio)
        return duracao >= persistencia_minima_ms, duracao

    def limpar_sessao(self, session_id: str | None = None) -> None:
        """Limpa o estado de uma sessão; sem ID, limpa todo o estado temporal."""
        if session_id is None:
            self.ultimo_visto.clear()
            self.inicio_condicao.clear()
            return

        def pertence_a_sessao(chave: Hashable) -> bool:
            return isinstance(chave, tuple) and bool(chave) and chave[0] == session_id

        self.ultimo_visto = {
            chave: instante for chave, instante in self.ultimo_visto.items()
            if not pertence_a_sessao(chave)
        }
        self.inicio_condicao = {
            chave: instante for chave, instante in self.inicio_condicao.items()
            if not pertence_a_sessao(chave)
        }

    def processar(self, fluxo_eventos: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Filtra eventos de condição legados, sem descartar mensagens de sinal.

        Este método é mantido como API auxiliar. O pipeline S2 atual usa
        aplicar_debounce() somente quando decide emitir uma transição de evento.
        Cada item precisa ter timestamp/timestamp_ms e uma chave estável.
        """
        filtrados: list[dict[str, Any]] = []
        for item in fluxo_eventos:
            evento = dict(item)
            chave = evento.get("debounce_key")
            if chave is None:
                chave = (
                    evento.get("rule_id"),
                    evento.get("entity_id"),
                    evento.get("zone_id"),
                    evento.get("equipment_item"),
                )
            timestamp = evento.get("timestamp_ms", evento.get("captured_at", evento.get("timestamp")))
            ts_ms = self.timestamp_em_ms(timestamp)
            janela = evento.get("debounce_ms", self.debounce_window_ms)
            if self.aplicar_debounce(chave, ts_ms, int(janela)):
                filtrados.append(evento)
        return filtrados
