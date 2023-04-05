from datetime import datetime, timedelta

from auth0.authentication import GetToken
from auth0.exceptions import Auth0Error
from auth0.management import Auth0 as Auth0Management
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext

app = FastAPI()

SECRET_KEY = "Cloud-Computing-HW1"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Auth0 configuration
AUTH0_DOMAIN = 'dev-yf3s0lg5eryzqb8u.us.auth0.com'
AUTH0_CLIENT_ID = 'h4jdBFP3CTojgeTGlwGkZhS8YMUtt04E'
AUTH0_CLIENT_SECRET = 'jpvxkR1CGUOk1S6ACDhtqcmcNlLibiZroIAumN14pBbd1PHIkVm7lPYiUzRxz_iL'
AUTH0_AUDIENCE = 'https://dev-yf3s0lg5eryzqb8u.us.auth0.com/api/v2/'

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

get_token = GetToken(AUTH0_DOMAIN, client_id=AUTH0_CLIENT_ID, client_secret=AUTH0_CLIENT_SECRET)
token = get_token.client_credentials(audience=AUTH0_AUDIENCE)['access_token']
auth0 = Auth0Management(AUTH0_DOMAIN, token)


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
