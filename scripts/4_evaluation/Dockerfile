FROM python:3.8.15-slim
RUN mkdir -p /opt/starvers/eval/queries
COPY requirements.txt /opt/starvers/eval
COPY evaluate.py /opt/starvers/eval
WORKDIR /opt/starvers/eval
RUN pip install -r requirements.txt
