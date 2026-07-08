FROM python:3.12-alpine

WORKDIR /app
COPY embedding_sidecar.py /app/embedding_sidecar.py

ENV HOST=0.0.0.0
ENV PORT=8080
ENV UDB_EMBED_PROVIDER=deterministic
ENV UDB_EMBED_DIMS=16

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD python -c "import json,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)); raise SystemExit(0 if data.get('ok') else 1)"
CMD ["python", "/app/embedding_sidecar.py"]
