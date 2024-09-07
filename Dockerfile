FROM python:3.11-slim

WORKDIR .
COPY . .

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8080

CMD exec gunicorn --bind :8080 --workers 1 --timeout 3600 --threads 8 main:app --access-logfile - --log-level info
