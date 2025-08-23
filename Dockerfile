# ---------- First Stage: Python Backend ----------
FROM python:3.10 AS python-backend

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
COPY ./app /code/app
COPY ./evaluation /code/evaluation

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/code"

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

#CMD ["fastapi", "run", "app/main.py", "--port", "80"]
CMD ["uvicorn", "app.main:app", "--reload"]
