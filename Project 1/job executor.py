import logging
import time
from datetime import datetime

import pymysql
import requests

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

timeout = 50
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="defaultdb",
    host="mysql-2356b3c0-cloud-computing-app.aivencloud.com",
    password="AVNS_6piYB8BWUXyYQ2Udutn",
    read_timeout=timeout,
    port=24313,
    user="avnadmin",
    write_timeout=timeout,
)

# Create database connection
cursor = connection.cursor()

# Mailgun configurations
MAILGUN_API_KEY = 'e8e51d665fd3789aad045b5dc5c7a937-81bd92f8-5cb782a1'
MAILGUN_DOMAIN = 'https://api.mailgun.net/v3/sandboxd810fa8fbc0944a39c6fb2760433b07d.mailgun.org'
MAILGUN_SENDER = 'mailgun@sandboxd810fa8fbc0944a39c6fb2760433b07d.mailgun.org'

# Create database connection
cursor = connection.cursor()

# CodeX service URL
CODEX_SERVICE_URL = 'https://api.codex.jaagrav.in'


def send_email(to, subject, body):
    logger.info(f'Sending email to {to} with subject {subject}')
    return requests.post(
        f"{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        data={"from": f"Admin User <{MAILGUN_SENDER}>",
              "to": to,
              "subject": subject,
              "text": body})


def execute_job(query_string, id, upload_fk):
    # get the email of the user who uploaded the executable
    cursor.execute('SELECT email, file_name FROM uploads WHERE id=%s', (upload_fk,))
    details = cursor.fetchone()
    email = details['email']
    file_name = details['file_name']
    headers = {
        'Content-Type': 'application/json'
    }
    # Send request to CodeX service
    try:
        logger.info(f'Executing job {id} with query {query_string}')
        response = requests.post(CODEX_SERVICE_URL, data=query_string, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # Send error email to user and update job status
        send_email(email, f"Job {file_name} execution failed", f"Error message: {err}")
        cursor.execute('UPDATE jobs SET status=\'failed\' WHERE id=%s', (id,))
        cursor.execute('INSERT INTO results (job, output, status, execution_date) VALUES (%s, %s, %s, %s)',
                       (id, err, 'failed', datetime.now()))
        connection.commit()
        logger.error(f'Job {id} failed with error {err}')
        return

    # Send result email to user and update job status
    result = response.text
    send_email(email, f"Job {file_name} executed successfully", f"Output: {result}")
    cursor.execute('UPDATE jobs SET status=\'executed\' WHERE id=%s', (id,))
    cursor.execute('INSERT INTO results (job, output, status, execution_date) VALUES (%s, %s, %s, %s)',
                   (id, result, 'done', datetime.now()))
    connection.commit()
    logger.info(f'Job {id} executed successfully')


def run_service():
    logger.info('Starting job executor service')
    while True:
        logger.info('Checking for jobs to execute')
        connection.ping(reconnect=True)
        # Get undone jobs from database
        cursor.execute('SELECT * FROM jobs as j WHERE j.status=\'in-progress\'')
        undone_jobs = cursor.fetchall()

        # Execute each job
        for job in undone_jobs:
            logger.info(f'Executing job {job["id"]}')
            execute_job(job['job'], job['id'], job['upload'])

        # Wait for some time before checking again
        connection.commit()
        time.sleep(6)


if __name__ == '__main__':
    run_service()
