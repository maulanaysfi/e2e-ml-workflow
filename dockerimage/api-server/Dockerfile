FROM python:3.13.7-slim

WORKDIR /workdir

COPY ./requirements.txt .
COPY ./server-v2.py .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

#CMD ["python", "server.py"]
CMD ["uvicorn", "server-v2:app", "--host", "0.0.0.0", "--port", "5000"]
