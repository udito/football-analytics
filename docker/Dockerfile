FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Set the correct working directory for running the app
WORKDIR /app/src/app
CMD ["python3", "main.py"]