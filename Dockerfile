FROM python:3.10

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app/api /code/app/api
COPY ./app/enums /code/app/enums
# COPY ./app/gui /code/app/gui # Copy after the development is done
# COPY ./evaluation /code/evaluation # Copy after the development is done
COPY ./app/models /code/app/models
COPY ./app/services /code/app/services
COPY ./app/utils /code/app/utils
COPY ./app/__init__.py /code/app/__init__.py
COPY ./app/AppConfig.py /code/app/AppConfig.py
COPY ./app/Database.py /code/app/Database.py
COPY ./app/LoggingConfig.py /code/app/LoggingConfig.py
COPY ./app/main.py /code/app/main.py
COPY ./app/run_gui.py /code/app/run_gui.py

ENV PYTHONPATH "${PYTHONPATH}:/code"

CMD ["fastapi", "run", "app/main.py", "--port", "80"]