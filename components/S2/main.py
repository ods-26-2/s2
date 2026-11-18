
"""Ponto de entrada do componente S2 (arquitetura ODS 6c, interfaces 1.0).

Entrada: array de mensagens com envelope comum.

Saída: array de mensagens ods.s2.entity e ods.s2.event.

Artefatos:
- RULES: regras de processamento.
- PPE_CATALOG: catálogo de equipamentos de proteção individual.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


# Importações compatíveis com execução como pacote ou arquivo direto.
if __package__:
    from .config import (
        CAMINHO_ENTRADA_S2,
        CAMINHO_ENTRADA_S2_FUSAO,
        CAMINHO_REGRAS_S2,
        CAMINHO_REGRAS_S2_EXEMPLO,
        CAMINHO_PPE_CATALOG_S2,
        CAMINHO_PPE_CATALOG_S2_EXEMPLO,
        CAMINHO_SAIDA_S2,
    )
    from .filtro_ruido_temporal import FiltroRuidoTemporal
    from .motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente

else:
    from config import (
        CAMINHO_ENTRADA_S2,
        CAMINHO_ENTRADA_S2_FUSAO,
        CAMINHO_REGRAS_S2,
        CAMINHO_REGRAS_S2_EXEMPLO,
        CAMINHO_PPE_CATALOG_S2,
        CAMINHO_PPE_CATALOG_S2_EXEMPLO,
        CAMINHO_SAIDA_S2,
    )
    from filtro_ruido_temporal import FiltroRuidoTemporal
    from motor_estado_concorrente import MotorGerenciamentoEstadoConcorrente


class CamadaS2:
    """Orquestra os mecanismos temporais e o estado do componente S2."""

    
    def __init__(
        self,
        artefato_regras: dict[str, Any] | None = None,
        artefato_ppe_catalog: dict[str, Any] | None = None,
    ) -> None:
        self.filtro_temporal = FiltroRuidoTemporal()

        self.motor_estado = MotorGerenciamentoEstadoConcorrente(
            filtro_temporal=self.filtro_temporal,
            artefato_ppe_catalog=artefato_ppe_catalog,
        )

        if artefato_regras is not None:
            self.motor_estado.carregar_regras(artefato_regras)


    def processar_eventos(
        self,
        fluxo_bruto: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Processa mensagens de entrada conforme os contratos registrados."""
        return self.motor_estado.processar_mensagens(fluxo_bruto)


def _ler_json(caminho: Path) -> Any:
    """Lê um arquivo JSON e apresenta erros de sintaxe com localização."""
    try:
        with caminho.open("r", encoding="utf-8") as arquivo:
            return json.load(arquivo)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"JSON inválido em {caminho}, "
            f"linha {exc.lineno}, coluna {exc.colno}: {exc.msg}"
        ) from exc


def _ler_mensagens(caminho: Path) -> list[dict[str, Any]]:
    """Lê e valida a estrutura básica do arquivo de mensagens."""
    dados = _ler_json(caminho)

    if isinstance(dados, dict):
        if isinstance(dados.get("messages"), list):
            dados = dados["messages"]
        else:
            dados = [dados]

    if not isinstance(dados, list):
        raise ValueError(
            'A raiz do JSON de entrada deve ser uma mensagem, '
            'um array ou {"messages": [...]}'
        )

    if not all(isinstance(item, dict) for item in dados):
        raise ValueError(
            "Todos os itens do JSON de entrada devem ser "
            "objetos de mensagem"
        )

    return dados


def _ler_regras(
    caminho_solicitado: Path | None,
) -> tuple[dict[str, Any] | None, Path | None]:
    """Carrega o artefato RULES."""

    if caminho_solicitado is not None:
        if not caminho_solicitado.exists():
            raise FileNotFoundError(
                f"Artefato RULES não encontrado: {caminho_solicitado}"
            )

        dados = _ler_json(caminho_solicitado)

        if not isinstance(dados, dict):
            raise ValueError(
                "O artefato RULES deve ser um objeto JSON "
                "com artifact_version e rules"
            )

        return dados, caminho_solicitado

    if CAMINHO_REGRAS_S2.exists():
        dados = _ler_json(CAMINHO_REGRAS_S2)

        if not isinstance(dados, dict):
            raise ValueError(
                "O artefato RULES deve ser um objeto JSON: "
                f"{CAMINHO_REGRAS_S2}"
            )

        return dados, CAMINHO_REGRAS_S2

    if CAMINHO_REGRAS_S2_EXEMPLO.exists():
        dados = _ler_json(CAMINHO_REGRAS_S2_EXEMPLO)

        if not isinstance(dados, dict):
            raise ValueError(
                "O arquivo de exemplo RULES deve conter "
                "um objeto JSON"
            )

        print(
            f"[AVISO] {CAMINHO_REGRAS_S2} não existe; "
            f"usando regras demonstrativas de "
            f"{CAMINHO_REGRAS_S2_EXEMPLO}. "
            "Para uso real, forneça seu artefato RULES.",
            file=sys.stderr,
        )

        return dados, CAMINHO_REGRAS_S2_EXEMPLO

    return None, None


def _validar_ppe_catalog(dados: Any, caminho: Path) -> dict[str, Any]:
    """Valida a estrutura básica do artefato PPE_CATALOG."""

    if not isinstance(dados, dict):
        raise ValueError(
            f"O PPE_CATALOG deve ser um objeto JSON: {caminho}"
        )

    if not isinstance(dados.get("catalog_version"), str):
        raise ValueError(
            f"O PPE_CATALOG de {caminho} deve possuir "
            "catalog_version como texto"
        )

    if not isinstance(dados.get("items"), list):
        raise ValueError(
            f"O PPE_CATALOG de {caminho} deve possuir "
            "items como lista"
        )

    if not isinstance(dados.get("required"), list):
        raise ValueError(
            f"O PPE_CATALOG de {caminho} deve possuir "
            "required como lista"
        )

    return dados


def _ler_ppe_catalog(
    caminho_solicitado: Path | None,
) -> tuple[dict[str, Any] | None, Path | None]:
    """Carrega o artefato PPE_CATALOG.

    Ordem de procura:
    1. Caminho informado explicitamente por --ppe-catalog.
    2. Artefato PPE_CATALOG padrão.
    3. Arquivo demonstrativo.

    Se nenhum arquivo existir, o S2 será executado sem catálogo.
    """

    if caminho_solicitado is not None:
        if not caminho_solicitado.exists():
            raise FileNotFoundError(
                f"Artefato PPE_CATALOG não encontrado: "
                f"{caminho_solicitado}"
            )

        dados = _validar_ppe_catalog(
            _ler_json(caminho_solicitado),
            caminho_solicitado,
        )

        return dados, caminho_solicitado

    if CAMINHO_PPE_CATALOG_S2.exists():
        dados = _validar_ppe_catalog(
            _ler_json(CAMINHO_PPE_CATALOG_S2),
            CAMINHO_PPE_CATALOG_S2,
        )

        return dados, CAMINHO_PPE_CATALOG_S2

    if CAMINHO_PPE_CATALOG_S2_EXEMPLO.exists():
        dados = _validar_ppe_catalog(
            _ler_json(CAMINHO_PPE_CATALOG_S2_EXEMPLO),
            CAMINHO_PPE_CATALOG_S2_EXEMPLO,
        )

        print(
            f"[AVISO] {CAMINHO_PPE_CATALOG_S2} não existe; "
            f"usando catálogo demonstrativo de "
            f"{CAMINHO_PPE_CATALOG_S2_EXEMPLO}. "
            "Para uso real, forneça seu artefato PPE_CATALOG.",
            file=sys.stderr,
        )

        return dados, CAMINHO_PPE_CATALOG_S2_EXEMPLO

    return None, None


def executar(
    caminho_entrada: Path,
    caminho_saida: Path,
    caminho_regras: Path | None = None,
    caminho_ppe_catalog: Path | None = None,
) -> int:
    """Executa o processamento completo do componente S2."""

    if not caminho_entrada.exists():
        print(
            f"[ERRO] Arquivo de entrada não encontrado: "
            f"{caminho_entrada}",
            file=sys.stderr,
        )
        return 2

    try:
        # 1. Leitura das mensagens de entrada.
        mensagens = _ler_mensagens(caminho_entrada)

        # 2. Carregamento dos artefatos.
        artefato_regras, origem_regras = _ler_regras(
            caminho_regras
        )

        artefato_ppe_catalog, origem_ppe_catalog = (
            _ler_ppe_catalog(caminho_ppe_catalog)
        )

        # 3. Inicialização da camada S2.
        s2 = CamadaS2(
            artefato_regras=artefato_regras,
            artefato_ppe_catalog=artefato_ppe_catalog,
        )

        # 4. Processamento das mensagens.
        mensagens_saida = s2.processar_eventos(mensagens)

        if not isinstance(mensagens_saida, list):
            raise TypeError(
                "O motor S2 deveria retornar uma lista de mensagens"
            )

        if not all(
            isinstance(item, dict)
            for item in mensagens_saida
        ):
            raise TypeError(
                "O motor S2 retornou itens que não são "
                "objetos de mensagem"
            )

        # 5. Criação do diretório de saída.
        caminho_saida.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # 6. Gravação do resultado.
        with caminho_saida.open(
            "w",
            encoding="utf-8",
        ) as arquivo:
            json.dump(
                mensagens_saida,
                arquivo,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

            arquivo.write("\n")

    except (OSError, ValueError, TypeError) as exc:
        print(
            f"[ERRO] {exc}",
            file=sys.stderr,
        )
        return 2

    # 7. Resumo da execução.
    contagem = Counter(
        str(item.get("schema", "schema_desconhecido"))
        for item in mensagens_saida
    )

    print(
        f"Entrada: {caminho_entrada} "
        f"({len(mensagens)} mensagens)"
    )

    print(
        "Regras: "
        f"{origem_regras if origem_regras else 'nenhum artefato RULES carregado'}"
    )

    print(
        "PPE_CATALOG: "
        f"{origem_ppe_catalog if origem_ppe_catalog else 'nenhum catálogo carregado'}"
    )

    if artefato_ppe_catalog is not None:
        print(
            "Versão do PPE_CATALOG: "
            f"{artefato_ppe_catalog['catalog_version']}"
        )

    print(
        f"Saída: {caminho_saida} "
        f"({len(mensagens_saida)} mensagens)"
    )

    for schema, quantidade in sorted(contagem.items()):
        print(f"  {schema}: {quantidade}")

    return 0


def main() -> int:
    """Configura os argumentos e inicia a execução do S2."""

    parser = argparse.ArgumentParser(
        description=(
            "Executa o componente S2 da arquitetura ODS 6c."
        )
    )

    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=CAMINHO_ENTRADA_S2,
        help=(
            "JSON de mensagens de entrada "
            f"(padrão: {CAMINHO_ENTRADA_S2})"
        ),
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=CAMINHO_SAIDA_S2,
        help=(
            "JSON de mensagens ods.s2.entity/event "
            f"(padrão: {CAMINHO_SAIDA_S2})"
        ),
    )

    parser.add_argument(
        "--rules",
        "-r",
        type=Path,
        default=None,
        help=(
            "Artefato RULES; por padrão procura "
            f"{CAMINHO_REGRAS_S2} e depois "
            f"{CAMINHO_REGRAS_S2_EXEMPLO}"
        ),
    )

    parser.add_argument(
        "--ppe-catalog",
        type=Path,
        default=None,
        help=(
            "Artefato PPE_CATALOG; por padrão procura "
            f"{CAMINHO_PPE_CATALOG_S2} e depois "
            f"{CAMINHO_PPE_CATALOG_S2_EXEMPLO}"
        ),
    )

    parser.add_argument(
        "--fusao-mock",
        action="store_true",
        help=(
            "Usa o JSON demonstrativo de duas câmeras "
            "gerado por gerar_mocks.py, caso a entrada "
            "padrão esteja selecionada."
        ),
    )

    args = parser.parse_args()

    entrada = args.input

    if (
        args.fusao_mock
        and args.input == CAMINHO_ENTRADA_S2
    ):
        entrada = CAMINHO_ENTRADA_S2_FUSAO

    return executar(
        caminho_entrada=entrada,
        caminho_saida=args.output,
        caminho_regras=args.rules,
        caminho_ppe_catalog=args.ppe_catalog,
    )


if __name__ == "__main__":
    raise SystemExit(main())
