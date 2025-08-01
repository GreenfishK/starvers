# ---------- First Stage: Python Backend ----------
FROM python:3.10 AS python-backend

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

# ---------- Second Stage: Java RDF Validator ----------
FROM maven:3.9.6-eclipse-temurin-11 AS rdfvalidator

# Copy only the validator source and POM
COPY ./app/utils/RDFValidator /code/app/utils/RDFValidator
WORKDIR /code/app/utils/RDFValidator

# Build the shaded JAR with all dependencies
RUN mvn clean compile assembly:single


# ---------- Final Image ----------
FROM python:3.10

WORKDIR /code

# Copy Python app from python-backend stage
COPY --from=python-backend /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=python-backend /usr/local/bin /usr/local/bin 
COPY --from=python-backend /code /code

# Copy compiled validator JAR from rdfvalidator stage

COPY --from=rdfvalidator /code/app/utils/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar /code/app/utils/rdfvalidator-1.0-jar-with-dependencies.jar

ENV PYTHONPATH="${PYTHONPATH}:/code"

CMD ["fastapi", "run", "app/main.py", "--port", "80"]
