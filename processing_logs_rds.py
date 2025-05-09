from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from confluent_kafka import KafkaError
import psycopg2
from produce_logs import create_secret_manager
import logging
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



# Function to create connection to AWS RDS PostgreSQL
def create_rds_connection():
    """Establish a connection to AWS RDS PostgreSQL."""
    secrets = create_secret_manager("MWAA_Secrets_V2")

    try:
        conn = psycopg2.connect(
            host=secrets['RDS_HOST'],
            database=secrets['RDS_DB_NAME'],
            user=secrets['RDS_USER'],
            password=secrets['RDS_PASSWORD'],
            port=secrets['RDS_PORT']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to insert parsed data into PostgreSQL
def insert_data_into_rds(data):
    """Insert the parsed log data into the PostgreSQL database."""
    conn = create_rds_connection()
    if conn is None:
        logger.error("Failed to connect to RDS. Aborting insertion.")
        return

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO logs (ip, timestamp, method, endpoint, protocol, status, size, referrer, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    values = (
        data['ip'],
        data['@timestamp'],
        data['method'],
        data['endpoint'],
        data['protocol'],
        data['status'],
        data['size'],
        data['referrer'],
        data['user_agent']
    )

    try:
        cursor.execute(insert_query, values)
        conn.commit()
        logger.info(f"Inserted log data for IP {data['ip']} into RDS.")
    except Exception as e:
        logger.error(f"Error inserting data into RDS: {e}")
    finally:
        cursor.close()
        conn.close()



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


    consumer = Consumer(consumer_config)
    consumer.subscribe([secrets['KAFKA_TOPIC']])

    try:
        while True:
            msg = consumer.poll(1.0) 
            # If no messages continue the polling 
            if msg is None:
                continue  
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition reached
                else:
                    raise KafkaException(msg.error())

            # Message received successfully
            message_value = msg.value().decode('utf-8')
            data = parse_log_entry(message_value)

            if data:
                # Insert the parsed log data into RDS
                insert_data_into_rds(data)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# DAG Configuration
default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'log_consumer_pipeline',
    default_args=default_args,
    description='Consume logs from Kafka and index to Elasticsearch',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['logs', 'kafka', 'elasticsearch']
)

consume_logs_task = PythonOperator(
    task_id='consume_and_index_logs',
    python_callable=consume_and_index_logs,
    dag=dag,
)

consume_logs_task




