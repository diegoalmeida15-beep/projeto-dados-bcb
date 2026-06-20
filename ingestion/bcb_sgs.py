"""
Ingestao de dados da API do Banco Central (SGS - Sistema Gerenciador de Series Temporais).

Serie 1 = Cambio USD/BRL (taxa de venda, diaria)
Documentacao: https://dadosabertos.bcb.gov.br/dataset/dolar-americano-usd-todos-os-boletins-diarios
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

SERIE_CODIGO = 1
BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
OUTPUT_DIR = Path("data/raw")


def buscar_serie(codigo: int, data_inicial: Optional[str] = None, data_final: Optional[str] = None) -> list:
    """Busca uma serie temporal do SGS. Datas no formato dd/mm/aaaa."""
    url = BASE_URL.format(codigo=codigo)
    params = {"formato": "json"}
    if data_inicial:
        params["dataInicial"] = data_inicial
    if data_final:
        params["dataFinal"] = data_final

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar API do BCB: {e}", file=sys.stderr)
        raise

    return resp.json()


def salvar_raw(dados: list, codigo: int) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = OUTPUT_DIR / f"sgs_{codigo}_{timestamp}.json"
    filepath.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
    return filepath


def main():
    hoje = datetime.now()
    data_inicial = (hoje - timedelta(days=30)).strftime("%d/%m/%Y")
    data_final = hoje.strftime("%d/%m/%Y")

    print(f"Buscando serie {SERIE_CODIGO} (USD/BRL) na API do BCB de {data_inicial} a {data_final}...")
    dados = buscar_serie(SERIE_CODIGO, data_inicial=data_inicial, data_final=data_final)
    print(f"{len(dados)} registros recebidos.")

    if not dados:
        print("Nenhum dado retornado. Encerrando sem gravar arquivo.")
        return

    filepath = salvar_raw(dados, SERIE_CODIGO)
    print(f"Dados salvos em: {filepath}")
    print(f"Exemplo do ultimo registro: {dados[-1]}")


if __name__ == "__main__":
    main()