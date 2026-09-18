FROM python:3.14-slim

RUN apt-get update && apt-get install -y postgresql-client

RUN pip install --no-cache-dir psycopg2-binary python-dotenv

COPY pull_script.py .

#! Not needed since this is in the compose file
# CMD ["python", "pull_script.py"]