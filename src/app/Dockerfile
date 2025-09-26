FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy source code
COPY . .
CMD ["python3", "main.py"]