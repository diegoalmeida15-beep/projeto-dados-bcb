"""
Ingestao de dados da API do Banco Central (SGS) com publicacao no Kafka (Confluent Cloud).

Serie 1 = Cambio USD/BRL (taxa de venda, diaria)
"""
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import requests
from confluent_kafka import Producer

SERIE_CODIGO = 1
TOPIC_NAME = "usd_brl_stream"
BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

# Credenciais via variavel de ambiente (nunca hardcoded no codigo)
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
}


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


def delivery_report(err, msg):
    """Callback chamado pelo Kafka apos cada tentativa de envio."""
    if err is not None:
        print(f"Falha ao entregar mensagem: {err}", file=sys.stderr)
    else:
        print(f"Mensagem entregue: partition={msg.partition()} offset={msg.offset()}")


def publicar_no_kafka(dados: list, producer: Producer, topic: str):
    for registro in dados:
        key = registro["data"]
        value = json.dumps(registro, ensure_ascii=False)

        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()


def main():
    hoje = datetime.now()
    data_inicial = (hoje - timedelta(days=30)).strftime("%d/%m/%Y")
    data_final = hoje.strftime("%d/%m/%Y")

    print(f"Buscando serie {SERIE_CODIGO} (USD/BRL) na API do BCB de {data_inicial} a {data_final}...")
    dados = buscar_serie(SERIE_CODIGO, data_inicial=data_inicial, data_final=data_final)
    print(f"{len(dados)} registros recebidos.")

    if not dados:
        print("Nenhum dado retornado. Encerrando sem publicar.")
        return

    producer = Producer(KAFKA_CONFIG)
    print(f"Publicando {len(dados)} registros no topico '{TOPIC_NAME}'...")
    publicar_no_kafka(dados, producer, TOPIC_NAME)
    print("Publicacao concluida.")


if __name__ == "__main__":
    main()