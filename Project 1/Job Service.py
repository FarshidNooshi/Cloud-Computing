from urllib.parse import urlencode

import pika
import psycopg2
import requests

# Set up RabbitMQ connection
cloudamqp_url = 'amqps://ghtiaznm:nfMSp4UtGzag-qUSjEL3z77lPIr14rpA@gull.rmq.cloudamqp.com/ghtiaznm'
params = pika.URLParameters(cloudamqp_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='file_creation')

# Set up database connection
db_url = 'mysql://avnadmin:AVNS_6piYB8BWUXyYQ2Udutn@mysql-2356b3c0-cloud-computing-app.aivencloud.com:24313/defaultdb?ssl-mode=REQUIRED'
conn = psycopg2.connect(db_url)
cursor = conn.cursor()


# Define a function to execute the file and store its contents in the database
def execute_file(file_id):
    # Get the executable file information from the database
    cursor.execute("SELECT * FROM executable_files WHERE id=%s", (file_id,))
    file_data = cursor.fetchone()

    # Check if the file exists
    if not file_data:
        return "File not found"

    # Execute the file and get its contents
    response = requests.get(file_data[1])
    file_contents = response.text

    # Store the file contents in the database
    params = {
        'file_id': file_id,
        'language': file_data[2],
        'contents': file_contents
    }
    query_string = urlencode(params)
    cursor.execute("INSERT INTO file_contents VALUES (%s, %s, %s)", (file_id, file_data[2], file_contents))
    conn.commit()

    return "File contents stored successfully"


# Define a function to handle incoming messages from RabbitMQ
def callback(ch, method, properties, body):
    # Get the file ID from the message body
    file_id = int(body.decode())

    # Execute the file and store its contents in the database
    result = execute_file(file_id)

    # Print the result
    print(result)

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages from RabbitMQ
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='file_creation', on_message_callback=callback)
channel.start_consuming()
