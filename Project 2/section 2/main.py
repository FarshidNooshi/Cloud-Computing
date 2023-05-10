from fastapi import FastAPI

app = FastAPI()
API_KEY = 'fcfe81da02864d3f8b8eee976e714666'

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
