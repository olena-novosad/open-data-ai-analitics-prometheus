FROM python:3.11-slim

WORKDIR /app

COPY data_quality_analysis/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_quality_analysis/app.py .

CMD ["python", "app.py"]