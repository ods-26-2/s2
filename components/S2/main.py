"""Ponto de entrada do componente S2 (arquitetura ODS 6c, interfaces 1.0).

Entrada: array de mensagens com envelope comum, normalmente ods.i2.tracks e
ods.s1.position. Saida: array de mensagens ods.s2.entity e ods.s2.event.
As regras sao lidas do artefato RULES informado por --rules.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

try:  # execução como pacote: python -m components.S2.main
    from .config import (
        CAMINHO_ENTRADA_S2, CAMINHO_ENTRADA_S2_FUSAO, CAMINHO_REGRAS_S2,
        CAMINHO_REGRAS_S2_EXEMPLO, CAMINHO_SAIDA_S2, DIRETORIO_SAIDA_S2,
        TREINO_MOT20_ESCOLHIDO,
    )
    from .filtro_ruido_temporal import FiltroRuidoTemporal
    from .motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente
except ImportError:  # execução direta a partir de components/S2
    from config import (
        CAMINHO_ENTRADA_S2, CAMINHO_ENTRADA_S2_FUSAO, CAMINHO_REGRAS_S2,
        CAMINHO_REGRAS_S2_EXEMPLO, CAMINHO_SAIDA_S2, DIRETORIO_SAIDA_S2,
        TREINO_MOT20_ESCOLHIDO,
    )
    from filtro_ruido_temporal import FiltroRuidoTemporal
    from motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente


class CamadaS2:
    """Orquestra os mecanismos temporais e o estado do componente S2."""

    def __init__(self, artefato_regras: dict[str, Any] | None = None) -> None:
        self.filtro_temporal = FiltroRuidoTemporal()
        self.motor_estado = MotorGerenciamentoEstadoConcorrente(
            filtro_temporal=self.filtro_temporal
        )
        if artefato_regras is not None:
            self.motor_estado.carregar_regras(artefato_regras)

    def processar_eventos(self, fluxo_bruto: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Processa mensagens de entrada conforme os contratos registrados."""
        return self.motor_estado.processar_mensagens(fluxo_bruto)


def _ler_json(caminho: Path) -> Any:
    try:
        with caminho.open("r", encoding="utf-8") as arquivo:
            return json.load(arquivo)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"JSON invalido em {caminho}, linha {exc.lineno}, coluna {exc.colno}: {exc.msg}"
        ) from exc


def _ler_mensagens(caminho: Path) -> list[dict[str, Any]]:
    dados = _ler_json(caminho)
    if isinstance(dados, dict):
        # Aceita envelope de lote explicito sem confundir com uma mensagem.
        if isinstance(dados.get("messages"), list):
            dados = dados["messages"]
        else:
            dados = [dados]
    if not isinstance(dados, list):
        raise ValueError("A raiz do JSON de entrada deve ser uma mensagem, um array ou {\"messages\": [...]}")
    if not all(isinstance(item, dict) for item in dados):
        raise ValueError("Todos os itens do JSON de entrada devem ser objetos de mensagem")
    return dados


def _ler_regras(caminho_solicitado: Path | None) -> tuple[dict[str, Any] | None, Path | None]:
    """Carrega RULES; se o arquivo de producao nao existir, usa exemplo de teste."""
    if caminho_solicitado is not None:
        if not caminho_solicitado.exists():
            raise FileNotFoundError(f"Artefato RULES nao encontrado: {caminho_solicitado}")
        dados = _ler_json(caminho_solicitado)
        if not isinstance(dados, dict):
            raise ValueError("O artefato RULES deve ser um objeto JSON com artifact_version e rules")
        return dados, caminho_solicitado

    if CAMINHO_REGRAS_S2.exists():
        dados = _ler_json(CAMINHO_REGRAS_S2)
        if not isinstance(dados, dict):
            raise ValueError(f"O artefato RULES deve ser um objeto JSON: {CAMINHO_REGRAS_S2}")
        return dados, CAMINHO_REGRAS_S2

    if CAMINHO_REGRAS_S2_EXEMPLO.exists():
        dados = _ler_json(CAMINHO_REGRAS_S2_EXEMPLO)
        if not isinstance(dados, dict):
            raise ValueError("O arquivo de exemplo RULES deve conter um objeto JSON")
        print(
            f"[AVISO] {CAMINHO_REGRAS_S2} nao existe; usando regras demonstrativas de "
            f"{CAMINHO_REGRAS_S2_EXEMPLO}. Para uso real, forneca seu artefato RULES.",
            file=sys.stderr,
        )
        return dados, CAMINHO_REGRAS_S2_EXEMPLO

    return None, None


def executar(caminho_entrada: Path, caminho_saida: Path, caminho_regras: Path | None = None) -> int:
    if not caminho_entrada.exists():
        print(f"[ERRO] Arquivo de entrada nao encontrado: {caminho_entrada}", file=sys.stderr)
        return 2

    try:
        mensagens = _ler_mensagens(caminho_entrada)
        artefato_regras, origem_regras = _ler_regras(caminho_regras)
        s2 = CamadaS2(artefato_regras)
        mensagens_saida = s2.processar_eventos(mensagens)
    except (OSError, ValueError, TypeError) as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        return 2

    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    with caminho_saida.open("w", encoding="utf-8") as arquivo:
        json.dump(mensagens_saida, arquivo, indent=2, ensure_ascii=False)
        arquivo.write("\n")

    contagem = Counter(item.get("schema", "schema_desconhecido") for item in mensagens_saida)
    print(f"Entrada: {caminho_entrada} ({len(mensagens)} mensagens)")
    print(f"Regras: {origem_regras if origem_regras else 'nenhum artefato RULES carregado'}")
    print(f"Saida: {caminho_saida} ({len(mensagens_saida)} mensagens)")
    for schema, quantidade in sorted(contagem.items()):
        print(f"  {schema}: {quantidade}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa o componente S2 da arquitetura ODS 6c.")
    parser.add_argument(
        "--input", "-i", type=Path, default=CAMINHO_ENTRADA_S2,
        help=f"JSON de mensagens de entrada (padrao: {CAMINHO_ENTRADA_S2})",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=CAMINHO_SAIDA_S2,
        help=f"JSON de mensagens ods.s2.entity/event (padrao: {CAMINHO_SAIDA_S2})",
    )
    parser.add_argument(
        "--rules", "-r", type=Path, default=None,
        help=f"Artefato RULES; por padrao procura {CAMINHO_REGRAS_S2} e depois {CAMINHO_REGRAS_S2_EXEMPLO}",
    )
    parser.add_argument(
        "--fusao-mock", action="store_true",
        help="Usa o JSON demonstrativo de duas cameras gerado por gerar_mocks.py",
    )
    args = parser.parse_args()
    entrada = args.input
    if args.fusao_mock and args.input == CAMINHO_ENTRADA_S2:
        entrada = CAMINHO_ENTRADA_S2_FUSAO
    return executar(entrada, args.output, args.rules)


if __name__ == "__main__":
    raise SystemExit(main())
