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
RABBITMQ_URL = os.environ['RABBITMQ_URL']
RABBITMQ_QUEUE = os.environ['RABBITMQ_QUEUE']

# Avien DB configurations
AVIEN_DB_HOST = os.environ['AVIEN_DB_HOST']
AVIEN_DB_PORT = os.environ['AVIEN_DB_PORT']
AVIEN_DB_NAME = os.environ['AVIEN_DB_NAME']
AVIEN_DB_USER = os.environ['AVIEN_DB_USER']
AVIEN_DB_PASSWORD = os.environ['AVIEN_DB_PASSWORD']

# Mailgun configurations
MAILGUN_API_KEY = os.environ['MAILGUN_API_KEY']
MAILGUN_DOMAIN = os.environ['MAILGUN_DOMAIN']
MAILGUN_SENDER = os.environ['MAILGUN_SENDER']

# S3 configurations
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
S3_REGION_NAME = os.environ['S3_REGION_NAME']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']

# CodeX service URL
CODEX_SERVICE_URL = os.environ['CODEX_SERVICE_URL']

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
