import json

import boto3
import pika
import pymysql

# S3 Configuration
S3_ACCESS_KEY = '11f00242-2593-4a34-bdb8-c45074b28ccc'
S3_SECRET_KEY = '95b771c08e979dae3bf7f8ca946fcac7a639d619'
S3_REGION_NAME = 's3.ir-thr-at1.arvanstorage.com/'
S3_BUCKET_NAME = 'cchw1aut'

# database configuration
timeout = 10
connection = pymysql.connect(
    host='mysql-2356b3c0-cloud-computing-app.aivencloud.com',
    port=24313,
    user='avnadmin',
    password='AVNS_6piYB8BWUXyYQ2Udutn',
    db='defaultdb',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    connect_timeout=timeout,
    read_timeout=timeout,
    write_timeout=timeout
)

# cursor = connection.cursor()

# RabbitMQ Configuration
RABBITMQ_URL = 'amqps://ghtiaznm:nfMSp4UtGzag-qUSjEL3z77lPIr14rpA@gull.rmq.cloudamqp.com/ghtiaznm'

# Set up RabbitMQ connection
params = pika.URLParameters(RABBITMQ_URL)
params.socket_timeout = 5
rabbitMQ_connection = pika.BlockingConnection(params)
channel = rabbitMQ_connection.channel()

# Create S3 client
s3_client = boto3.resource(
    's3',
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    endpoint_url=f'https://{S3_REGION_NAME}'
)


# consume message from RabbitMQ
def create_executable(ch, method, properties, body):
    try:
        message = json.loads(body)
        file_id = message['file_id']
        unique_id = message['unique_id']
        connection.ping(reconnect=True)
        cursor = connection.cursor()

        print(f'Creating executable with id {file_id}')

        # read executable info from database
        cursor.execute('SELECT * FROM uploads WHERE id=%s AND enable=0', (file_id,))
        executable_info = cursor.fetchone()

        if executable_info is None:
            # executable does not exist in the database
            raise ValueError(f'Executable with id {file_id} does not exist')

        # write contents to database
        language = executable_info['language']
        inputs = executable_info['inputs']
        file = s3_client.Object(S3_BUCKET_NAME, executable_info['file_name'])
        # make the code like this: 'val = int(input("Enter your value: ")) + 5\nprint(val)'
        query_dict = {
            'code': file.get()['Body'].read().decode('utf - 8'),
            'input': inputs,
            'language': language
        }
        CodeXQueryString = json.dumps(query_dict)
        cursor.execute('INSERT INTO jobs (job, status, upload) VALUES (%s, %s, %s)',
                       (CodeXQueryString, 'in-progress', file_id))
        connection.commit()

        # log success
        print(f'Successfully created executable with id {file_id}')

    except Exception as e:
        # log error
        print(f'Error creating executable: {e}')

    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    print('Starting executable creation service')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='create_work', on_message_callback=create_executable)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
