import os
import requests
import redis
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Read configuration variables from separate config files or environment variables
VERT_NUMBER = int(os.environ.get("VERT_NUMBER", 8000))
CACHE_DURATION_MINUTES = int(os.environ.get("CACHE_DURATION_MINUTES", 5))
API_ENDPOINT = os.environ.get("API_ENDPOINT", "https://api.rebrandly.com/v1/links")
API_KEY = os.environ.get("API_KEY", "fcfe81da02864d3f8b8eee976e714666")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Initialize Redis connection
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


class URLRequest(BaseModel):
    longURL: str


def shorten_url(long_url):
    # Check if the long URL is cached in Redis
    cached_url = redis_client.get(long_url)

    if cached_url:
        # Return the cached URL and indicate it is cached
        return {
            "longURL": long_url,
            "shortURL": cached_url.decode(),
            "isCached": True,
            "hostname": os.uname().nodename,
        }

    # Create the request payload for Rebrandly API
    headers = {"Content-Type": "application/json", "apikey": API_KEY}
    data = {"destination": long_url}

    # Send the request to Rebrandly API to shorten the URL
    response = requests.post(API_ENDPOINT, headers=headers, json=data)
    if response.status_code == 200:
        # Extract the shortened URL from the response
        short_url = response.json()["shortUrl"]

        # Cache the long URL and its corresponding shortened URL in Redis
        redis_client.setex(long_url, CACHE_DURATION_MINUTES * 60, short_url)

        # Return the shortened URL and indicate it is not cached
        return {
            "longURL": long_url,
            "shortURL": short_url,
            "isCached": False,
            "hostname": os.uname().nodename,
        }

    # Return an error response if Rebrandly API request fails
    return {"error": "Failed to shorten URL"}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.post("/shorten_url")
def handle_shorten_url(url_request: URLRequest):
    long_url = url_request.longURL
    return shorten_url(long_url)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=VERT_NUMBER)
