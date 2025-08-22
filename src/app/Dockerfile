FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY src/app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source code
COPY src/app/ ./src/app/

# Set the correct working directory for running the app
WORKDIR /app/src/app
CMD ["python3", "main.py"]