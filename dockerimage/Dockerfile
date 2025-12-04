FROM python:3.13.7-trixie

WORKDIR /workdir

COPY ./requirements.txt .
# COPY ./test.py .

# RUN adduser --disabled-password kfpuser
# USER kfpuser
RUN apt-get update && apt-get install -y libgomp1
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

CMD ["python", "--version"]
