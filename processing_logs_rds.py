from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk
import psycopg2
from produce_logs import create_secret_manager
import json
import logging
import boto3
import re

logger = logging.getLogger(__name__)

def parse_log_entry(log_entry):
    """
    Parse Apache log entry into structured data
    """
    log_pattern = r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)" (?P<status>\d+) (?P<size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'

    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Invalid log format: {log_entry}")
        return None
    data = match.groupdict()
    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], "%b %d %Y, %H:%M:%S")
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None
    return data


def consume_and_index_logs(**context):
    """Consume logs from Kafka and index to Elasticsearch."""
    secrets = create_secret_manager("MWAA_Secrets_V2")

    # Kafka consumer configuration
    consumer_config = {
        "bootstrap.servers": secrets['KAFKA_BOOTSTRAP_SERVER'],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": secrets['KAFKA_SASL_USERNAME'],
        "sasl.password": secrets['KAFKA_SASL_PASSWORD'],
        "group.id": "airflow_log_indexer",
        "auto.offset.reset": "latest"
    }





