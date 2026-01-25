# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src
COPY data/ ./data

# Set environment variable for Python
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python3", "src/pipeline.py"]
