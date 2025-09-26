FROM python:3.10-slim

WORKDIR /streamlit

# Install dependencies
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy source code
COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "streamlit-statsbomb.py"]
