FROM python:3.11.13-slim-trixie

ENV PROJECT_ROOT="/app"

COPY ./pyproject.toml ./poetry.lock $PROJECT_ROOT/

WORKDIR $PROJECT_ROOT

RUN apt update && apt install -y make gcc python3-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip && \
    pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-directory

COPY . .
