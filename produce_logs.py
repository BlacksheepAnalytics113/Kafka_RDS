from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3.session
from confluent_kafka import Producer
import json
import random
import logging
import boto3
from faker import Faker

fake = Faker()
logger = logging.getLogger(__name__)

def create_secret_manager(secret_name,region = "us-east-1"):
    """
    Retrieve the credentials from AWS secret manager
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retrieval error: {e}")
        raise

def create_kafka_producer(config):
    """Create Kafka producer with configuration."""
    return Producer(config)

def Generate_data():
    """ 
    Generate a log entry to produce data 
    """
    methods = ["GET", "POST", "PUT", "DELETE"]
    endpoints = ["/api/users", "/home", "/about", "/contact", "/services"]
    statuses = [200, 301, 302, 400, 404, 500]
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    ]
    ip = fake.ipv4()
    timestamps = datetime.now().strftime("%b %d %Y, %H:%M:%S")
    methods = random.choice(methods)
    endpoint = random.choice(endpoint)
    status = random.choice(statuses)
    size = random.randint(1000)
    referrers = random.choice(referrers)
    user_agents = random.choice(user_agents)
    # Log entry has been created!
    log_entry = (
        f"{ip} - - [{timestamps}] \"{methods} {endpoint} HTTP/1.1\" {status} {size} "
        f"\"{referrers}\" \"{user_agents}\""
    )
    return log_entry

def delivery_isg(err,msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce log entries to Kafka
def produce_logs(**context):
    """Produce log entries to Kafka.
    2. Generate data and send the data to the kafka topic been created
    3. Ensure all messages are flushed 
    """
    kafka_config = context['task_instance'].xcom_pull(task_ids='get_kafka_config')
    producer = create_kafka_producer(kafka_config)
    topic = "stream"
    for _ in range(1000):  
        log_entry = Generate_data()
        producer.produce(topic, value=log_entry, callback=delivery_isg)
        logger.info(f"Produced log entry: {log_entry}")
    producer.flush()

# Fetch Kafka configuration from a file or secret manager
def read_config():
    """Reads the Kafka configuration from a client.properties file or returns defaults."""
    config = {
        "bootstrap.servers": "your_kafka_broker",
        "client.id": "python-producer"
    }
    # You can read from a properties file if necessary
    # with open("client.properties") as fh:
    #     for line in fh:
    #         line = line.strip()
    #         if len(line) != 0 and line[0] != "#":
    #             parameter, value = line.strip().split('=', 1)
    #             config[parameter] = value.strip()

    return config

# Set up Airflow DAG to run the producer function



