FROM python:3.11-slim

WORKDIR /app

COPY data_load/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org

COPY data_load/app.py /app/app.py

CMD ["python", "app.py"]