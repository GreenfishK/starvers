FROM python:3.11

WORKDIR /code

COPY src/starversserver/requirements.txt /code/requirements.txt
COPY src/starversserver/app /code/app
COPY src/starvers /code/app/utils/starvers
COPY evaluation/starversserver /code/evaluation

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

ENV PYTHONPATH="/code"

