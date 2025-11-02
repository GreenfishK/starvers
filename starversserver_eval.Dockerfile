# ---- Stage 1: Builder ----
FROM python:3.11 AS builder

WORKDIR /install

# Copy only requirements first to leverage Docker cache for dependency installation
COPY src/starversserver/requirements.txt .

# Install dependencies into a separate directory
RUN pip install --no-cache-dir --upgrade -r requirements.txt --target=/install/packages


# ---- Stage 2: Final Image ----
FROM python:3.11 AS final

WORKDIR /code

# Copy installed dependencies from builder
COPY --from=builder /install/packages /usr/local/lib/python3.11/site-packages

# Copy application source code
COPY src/starversserver/app /code/app
COPY src/starvers /code/app/utils/starvers
COPY evaluation/starversserver /code/evaluation

ENV PYTHONPATH="/code"

