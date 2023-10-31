FROM apache/airflow:2.7.1

# Copy requirements.txt file
COPY requirements.txt /

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Install tesseract-ocr
RUN apt-get update && apt-get -y install tesseract-ocr
