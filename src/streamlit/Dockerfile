FROM python:3.10-slim

WORKDIR /streamlit

# Install dependencies
COPY src/streamlit/requirements.txt /streamlit/requirements.txt
RUN pip install --no-cache-dir -r /streamlit/requirements.txt

# Copy source code
COPY src/streamlit/ ./src/streamlit/

# Set the correct working directory for running the app
WORKDIR /streamlit/src/streamlit
EXPOSE 8501

CMD ["streamlit", "run", "streamlit-statsbomb.py"]
