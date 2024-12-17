# Start from a lightweight Python image
FROM python:3.11-slim

# Set environment variables to ensure non-interactive installs
ENV DEBIAN_FRONTEND=noninteractive

# Update and install any necessary system dependencies for psycopg2 and ssl
# psycopg2 requires libpq-dev and build-essential to compile, also install ca-certificates for secure requests
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the requirements file first to leverage Docker's caching if dependencies haven't changed
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables if needed for your application
# ENV RABBITMQ_URL=amqps://...
# ENV PG_HOST=...
# etc.

# Run the worker
CMD ["python", "worker.py"]
