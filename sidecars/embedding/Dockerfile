FROM python:3.12-alpine

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY embedding_sidecar.py /app/embedding_sidecar.py
COPY embedding_consumer.py /app/embedding_consumer.py

ENV HOST=0.0.0.0
ENV PORT=8080
ENV UDB_EMBED_PROVIDER=deterministic
ENV UDB_EMBED_DIMS=16
# http (default) | consumer — consumer mode reads udb.embedding.work.v1 and
# reports to the broker; see README.md for the required env.
ENV UDB_EMBEDDING_SIDECAR_MODE=http

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD python -c "import json,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)); raise SystemExit(0 if data.get('ok') else 1)"
CMD ["python", "/app/embedding_sidecar.py"]
