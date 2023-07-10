import json
import uuid
from datetime import timedelta

import boto3
import pika
import pymysql
from auth0.authentication import GetToken
from auth0.exceptions import Auth0Error
from auth0.management import Auth0 as Auth0Management
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, UploadFile, File
from fastapi import FastAPI
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext

app = FastAPI()

SECRET_KEY = "Cloud-Computing-HW1"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Auth0 configuration
AUTH0_DOMAIN = 'SECRET'
AUTH0_CLIENT_ID = 'SECRET'
AUTH0_CLIENT_SECRET = 'SECRET'
AUTH0_AUDIENCE = 'SECRET'

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

# S3 Configuration
S3_ACCESS_KEY = 'SECRET'
S3_SECRET_KEY = 'SECRET'
S3_REGION_NAME = 'SECRET'
S3_BUCKET_NAME = 'SECRET'

# RabbitMQ Configuration
RABBITMQ_URL = 'SECRET'

# Set up RabbitMQ connection
params = pika.URLParameters(RABBITMQ_URL)
params.socket_timeout = 5
rabbitMQ_connection = pika.BlockingConnection(params)
channel = rabbitMQ_connection.channel()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

get_token = GetToken(AUTH0_DOMAIN, client_id=AUTH0_CLIENT_ID, client_secret=AUTH0_CLIENT_SECRET)
token = get_token.client_credentials(audience=AUTH0_AUDIENCE)['access_token']
auth0 = Auth0Management(AUTH0_DOMAIN, token)

# Create S3 client
s3_client = boto3.resource(
    's3',
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    endpoint_url=f'https://{S3_REGION_NAME}'
)

# Create database connection
cursor = connection.cursor()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def authenticate_user(email: str, password: str):
    try:
        users = auth0.users.list(q=f'email:"{email}"')
        if len(users['users']) == 0:
            return False
        user = users['users'][0]
        hashed_password = user['user_metadata']['password_hash']
        if not verify_password(password, hashed_password):
            return False
        return {
            'email': user['email'],
            'user_id': user['user_id']
        }
    except Auth0Error:
        return False


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["email"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/users")
async def create_user(data: dict):
    email = data['email']
    password = data['password']
    hashed_password = get_password_hash(password)
    try:
        user_data = {
            'email': email,
            'password': password,
            'connection': 'Username-Password-Authentication',
            'user_metadata': {
                'password_hash': hashed_password
            }
        }
        user = auth0.users.create(user_data)
        return {"email": email, "token": token}
    except Auth0Error:
        raise HTTPException(status_code=500, detail="Error creating user")


@app.post("/program")
async def create_program(inputs: str, language: str, file: UploadFile = File(...), token: str = Depends(oauth2_scheme)):
    file_name = file.filename

    # Verify authentication token and get user email
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = decoded_token["sub"]
    except:
        raise HTTPException(status_code=401, detail="Invalid authentication token")

    # Save file to S3
    try:
        file_content = await file.read()
        s3_client.Bucket(S3_BUCKET_NAME).put_object(Key=file_name, Body=file_content, ACL='public-read')
    except ClientError as e:
        raise HTTPException(status_code=500, detail="Error saving file to S3")

    # Store program info in database
    try:
        # Use DBaaS client to connect to Aiven database
        # Replace with your own Aiven client configuration

        # Store program info in database
        # Replace with your own database table schema and query
        cursor.execute(
            "INSERT INTO uploads (email, inputs, language, enable, file_name) VALUES (%s, %s, %s, %s, %s)",
            (
                email, inputs, language, 0, file_name)
        )
        connection.commit()
    except:
        raise HTTPException(status_code=500, detail="Error storing program info in database")

    return {"message": "Program created successfully"}


@app.post("/create_work")
async def create_work(file_id: int, token: str = Depends(oauth2_scheme)):
    # Verify authentication token and get user email
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = decoded_token["sub"]
    except:
        raise HTTPException(status_code=401, detail="Invalid authentication token")

    cursor.execute("SELECT * FROM uploads WHERE id = %s", (file_id,))
    file_data = cursor.fetchone()
    if file_data is None:
        raise HTTPException(status_code=404, detail="File not found")

    if file_data['enable'] == 1:
        return {"message": "Execution request was not created"}

    # Generate unique ID
    unique_id = uuid.uuid4().hex

    # Send unique ID to second service using RabbitMQ
    channel.queue_declare(queue='create_work')
    channel.basic_publish(
        exchange='',
        routing_key='create_work',
        body=json.dumps({
            "file_id": file_id,
            "unique_id": unique_id
        })
    )
    connection.commit()

    return {"message": "Execution request created", "unique_id": unique_id}


from datetime import datetime


@app.get("/executions")
async def get_executions(token: str = Depends(oauth2_scheme)):
    # Verify authentication token and get user email
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = decoded_token["sub"]
    except:
        raise HTTPException(status_code=401, detail="Invalid authentication token")

    try:
        cursor.execute(
            "SELECT u.email, j.job, r.output, j.id as execution_id, u.file_name, r.execution_date, r.status, r.output as output FROM jobs j JOIN uploads u ON j.upload = u.id JOIN results r ON j.id = r.job WHERE u.email = %s",
            (email,))
        results = cursor.fetchall()
        executions = []
        for result in results:
            execution = {
                "execution_id": result["execution_id"],
                "file_link": f"https://{S3_BUCKET_NAME}.ir.{S3_REGION_NAME}/{result['file_name']}",
                "request_time": result["execution_date"].strftime("%Y-%m-%d %H:%M:%S"),
                "status": result["status"]
            }
            if result["status"] == "done":
                execution["result"] = result["output"]
                execution['error'] = None
                execution["execution_time"] = result["execution_date"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                execution["result"] = None
                execution['error'] = result['output']
                execution["execution_time"] = None
            executions.append(execution)
        return executions
    except:
        return []
