# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python code into the container
COPY . .

# Expose the port on which the server will listen
EXPOSE 8000

# Set the environment variables
ENV VERT_NUMBER=8000 \
    CACHE_DURATION_MINUTES=5 \
    API_ENDPOINT=https://api.rebrandly.com/v1/links \
    API_KEY=fcfe81da02864d3f8b8eee976e714666 \
    REDIS_HOST=redis \
    REDIS_PORT=6379

# Run the Python application
CMD ["python", "main.py"]
