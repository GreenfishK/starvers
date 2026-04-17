FROM python:3.11-slim
WORKDIR /app
RUN pip install flask --no-cache-dir
COPY index.html api.py ./
ENV DATA_DIR=/mnt/data/starvers_eval
ENV PORT=8080
EXPOSE 8080
CMD ["python", "api.py"]
