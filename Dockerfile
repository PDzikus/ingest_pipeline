FROM python:3.8

RUN mkdir -p /ingest_pipeline
COPY main /ingest_pipeline/main
COPY data /ingest_pipeline/data
COPY ./pyproject.toml ./poetry.lock /ingest_pipeline/

WORKDIR /ingest_pipeline

ENV MY_ENV=dev \
    POETRY_VERSION=1.0.0 \
    PYTHONPATH=/ingest_pipeline \
    PIP_NO_CACHE_DIR=off

RUN pip install "poetry==$POETRY_VERSION"
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$YOUR_ENV" == production && echo "--no-dev") --no-interaction --no-ansi
