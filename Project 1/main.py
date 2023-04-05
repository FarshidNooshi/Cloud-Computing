from typing import Optional

import requests
from authlib.jose import jwt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()


# Define a model for the user data
class User(BaseModel):
    email: str
    password: str


# Define a function to authenticate the user using Auth0
def authenticate_user(email: str, password: str) -> Optional[str]:
    # Set the Auth0 parameters
    auth0_domain = 'dev-yf3s0lg5eryzqb8u.us.auth0.com'
    auth0_audience = 'YOUR_AUTH0_AUDIENCE'
    auth0_client_id = 'fW0cnOXSB0XflmXGcD4mvdcLHQc0bzF2'
    auth0_client_secret = 'g__0UWe5sUps8UNlAxUPA7USnB2TA4Y4WMkQywDIaWHhrX26vmC6MqcKhYyzmFce'

    # Build the Auth0 API request URL
    auth0_url = f'https://{auth0_domain}/oauth/token'

    # Set the Auth0 API request data
    auth0_data = {
        'grant_type': 'password',
        'username': email,
        'password': password,
        'audience': auth0_audience,
        'client_id': auth0_client_id,
        'client_secret': auth0_client_secret
    }

    # Send the Auth0 API request
    auth0_response = requests.post(auth0_url, data=auth0_data)

    # Check if the Auth0 API request was successful
    if auth0_response.status_code == 200:
        # Extract the JWT token from the Auth0 API response
        auth0_token = auth0_response.json()['access_token']

        # Decode the JWT token to get the user email
        decoded_token = jwt.decode(auth0_token, algorithms=['RS256'], audience=auth0_audience,
                                   options={'verify_signature': False})
        user_email = decoded_token.get('email')

        # Return the user email and Auth0 JWT token
        return {'email': user_email, 'token': auth0_token}

    # Raise an exception if the Auth0 API request failed
    else:
        raise HTTPException(status_code=401, detail='Invalid credentials')


# Define a route for user authentication
@app.post('/auth')
def authenticate_user_route(user: User):
    # Try to authenticate the user using Auth0
    auth_result = authenticate_user(user.email, user.password)

    # Return the user email and Auth0 JWT token if authentication was successful
    if auth_result is not None:
        return auth_result

    # Raise an exception if authentication failed
    else:
        raise HTTPException(status_code=401, detail='Invalid credentials')
