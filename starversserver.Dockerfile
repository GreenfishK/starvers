# ---------- First Stage: Python Backend ----------
FROM python:3.11 AS python-backend

WORKDIR /code

COPY src/starversserver/requirements.txt /code/requirements.txt
COPY src/starversserver/app /code/app
COPY src/starvers /code/app/utils/starvers

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

ENV PYTHONPATH="/code"

# ---------- Second Stage: Java RDF Validator ----------
FROM maven:3.9.6-eclipse-temurin-11 AS rdfvalidator

# Copy only the validator source and POM
COPY --from=python-backend /code/app/utils/RDFValidator /code/app/utils/RDFValidator
WORKDIR /code/app/utils/RDFValidator

# Build the shaded JAR with all dependencies
RUN mvn clean compile assembly:single


# ---------- Final Image ----------
FROM python:3.11

WORKDIR /code

# Copy Python app from python-backend stage
COPY --from=python-backend /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=python-backend /usr/local/bin /usr/local/bin 
COPY --from=python-backend /code /code

# Copy compiled validator JAR from rdfvalidator stage
COPY --from=rdfvalidator /code/app/utils/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar /code/app/utils/rdfvalidator-1.0-jar-with-dependencies.jar

COPY src/starversserver/app/gui /code/app/gui
COPY src/starversserver/app/AppConfig.py /code/app/AppConfig.py
COPY src/starversserver/app/LoggingConfig.py /code/app/LoggingConfig.py

ENV PYTHONPATH="/code"

#CMD ["fastapi", "run", "app/main.py", "--port", "80"]
CMD ["uvicorn", "app.main:app", "--reload"]
