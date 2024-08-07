FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN python3 -m venv fastapi-env
RUN source fastapi-env/bin/activate

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY /app/ .

CMD ["fastapi", "run", "app/main.py", "--port", "80"]