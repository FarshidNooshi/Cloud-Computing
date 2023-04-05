import logging
import os

import boto3
import pika
import psycopg2
import requests
from mailgun import Mailgun

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configurations
RABBITMQ_URL = 'amqps://ghtiaznm:nfMSp4UtGzag-qUSjEL3z77lPIr14rpA@gull.rmq.cloudamqp.com/ghtiaznm'

# Avien DB configurations
AVIEN_DB_HOST = 'mysql-2356b3c0-cloud-computing-app.aivencloud.com'
AVIEN_DB_PORT = '24313'
AVIEN_DB_NAME = 'defaultdb'
AVIEN_DB_USER = 'avnadmin'
AVIEN_DB_PASSWORD = 'AVNS_6piYB8BWUXyYQ2Udutn'

# Mailgun configurations
MAILGUN_API_KEY = 'e8e51d665fd3789aad045b5dc5c7a937-81bd92f8-5cb782a1'
MAILGUN_DOMAIN = 'https://api.mailgun.net/v3/sandboxd810fa8fbc0944a39c6fb2760433b07d.mailgun.org'
MAILGUN_SENDER = 'mailgun@sandboxd810fa8fbc0944a39c6fb2760433b07d.mailgun.org'

# S3 configurations
S3_ACCESS_KEY = '11f00242-2593-4a34-bdb8-c45074b28ccc'
S3_SECRET_KEY = '95b771c08e979dae3bf7f8ca946fcac7a639d619'
S3_REGION_NAME = 's3.ir-thr-at1.arvanstorage.ir'

# CodeX service URL
CODEX_SERVICE_URL = 'https://api.codex.jaagrav.in'

# Create a connection to RabbitMQ
rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
rabbitmq_channel = rabbitmq_connection.channel()

# Create a connection to Avien DB
avien_db_connection = psycopg2.connect(
    host=AVIEN_DB_HOST,
    port=AVIEN_DB_PORT,
    dbname=AVIEN_DB_NAME,
    user=AVIEN_DB_USER,
    password=AVIEN_DB_PASSWORD
)
avien_db_cursor = avien_db_connection.cursor()

# Create a connection to S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION_NAME
)

# Create a Mailgun client
mailgun = Mailgun(api_key=MAILGUN_API_KEY, domain=MAILGUN_DOMAIN)


def execute_job(job_id):
    """
    Executes the job with the given ID by sending an HTTP request to the CodeX service.
    Sends an email to the user if there is an error in the execution and changes the job status to executed.
    """
    # Get the job information from Avien DB
    avien_db_cursor.execute('SELECT * FROM jobs WHERE id=%s', (job_id,))
    job = avien_db_cursor.fetchone()
    if job is None:
        logger.error(f'Job with ID {job_id} not found')
        return

    # Check if the job is not executed yet
    if job[4] != 'none':
        logger.warning(f'Job with ID {job_id} has already been executed')
        return

    # Download the executable file from S3
    s3_object = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=job[2])
    executable_content = s3_object['Body'].read().decode('utf-8')

    # Send an HTTP request to the CodeX service
    response = requests.post(CODEX_SERVICE_URL, json={
        'executable': executable_content,
        'language':
            job[3],
        'input':
            job[5]
    })

    # Check if the request was successful
    if response.status_code != 200:
        logger.error(f'Job with ID {job_id} failed to execute')
        mailgun.send_email(
            to=job[1],
            subject='Job Execution Failed',
            text=f'Job with ID {job_id} failed to execute'
        )
        avien_db_cursor.execute('UPDATE jobs SET status=%s WHERE id=%s', ('failed', job_id))
        avien_db_connection.commit()
        return None

    # Get the output from the response
    output = response.json()['output']

    # Update the job status to executed
    avien_db_cursor.execute('UPDATE jobs SET status=%s WHERE id=%s', ('executed', job_id))
    avien_db_connection.commit()

    # Send an email to the user
    mailgun.send_email(
        to=job[1],
        subject='Job Execution Completed',
        text=f'Job with ID {job_id} has been executed successfully. The output is:\n\n{output}'
    )

    return output
