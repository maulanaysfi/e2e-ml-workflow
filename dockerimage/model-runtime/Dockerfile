FROM python:3.13.7-trixie

WORKDIR /workdir

COPY ./requirements.txt .
COPY ./server-v2.py app.py

RUN apt-get update && apt-get install -y libgomp1
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5001"]
