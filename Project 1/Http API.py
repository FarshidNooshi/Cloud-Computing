from typing import Optional

import requests
from auth0.authentication import GetToken
from auth0.management import Auth0
from authlib.jose import jwt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()


# Define a model for the user data
class User(BaseModel):
    email: str
    password: str


# Define a function to authenticate the user using Auth0 and create the user if they don't exist
def authenticate_user(email: str, password: str) -> Optional[str]:
    # Set the Auth0 parameters
    auth0_domain = 'dev-yf3s0lg5eryzqb8u.us.auth0.com'
    auth0_audience = 'https://dev-yf3s0lg5eryzqb8u.us.auth0.com/api/v2/'
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

    print(auth0_response.json())

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

    # If the user doesn't exist, create a new user in Auth0 and return the JWT token
    elif auth0_response.status_code == 403 and 'invalid_grant' in auth0_response.json().get('error', ''):
        # Create a new Auth0 management API client
        auth0_token = GetToken(auth0_domain).client_credentials(auth0_client_id, auth0_client_secret,
                                                                f'https://{auth0_domain}/api/v2/')
        auth0 = Auth0(auth0_domain, token=auth0_token)

        # Create a new user in Auth0
        user_metadata = {'signup_method': 'local'}
        auth0.users.create({
            'email': email,
            'password': password,
            'connection': 'Username-Password-Authentication',
            'email_verified': False,
            'verify_email': True,
            'user_metadata': user_metadata
        })

        # Authenticate the user using Auth0
        return authenticate_user(email, password)

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
