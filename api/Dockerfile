FROM python:3.11-slim
WORKDIR /app
RUN pip install fastapi uvicorn kafka-python
COPY api_gateway.py .
CMD ["uvicorn", "api_gateway:app", "--host", "0.0.0.0", "--port", "8000"]
