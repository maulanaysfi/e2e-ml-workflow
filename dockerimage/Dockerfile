FROM python:3.13.7-slim

WORKDIR /workdir

COPY ./requirements.txt .
# COPY ./test.py .

# RUN adduser --disabled-password kfpuser
# USER kfpuser

RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

CMD ["python", "--version"]
