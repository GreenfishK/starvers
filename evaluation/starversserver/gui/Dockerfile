FROM python:3.11-slim
WORKDIR /app
RUN pip install flask --no-cache-dir
COPY index.html api.py ./
ENV DATA_DIR=/mnt/data/starversserver_eval
ENV PORT=8081
EXPOSE 8081
CMD ["python", "api.py"]
