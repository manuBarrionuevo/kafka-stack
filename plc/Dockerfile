FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python
COPY plc_emulator.py .
CMD ["python", "plc_emulator.py"]
