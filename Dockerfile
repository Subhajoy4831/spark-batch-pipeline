# Use official Python image
FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /app

# Copy dependency file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Default command
CMD ["python", "main.py"]