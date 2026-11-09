# The example client in a container, so the walk-through needs no local Python.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

USER nobody
ENTRYPOINT ["python", "main.py"]
