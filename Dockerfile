FROM python:3.10 as python-base

# Set Poetry's configs
ENV POETRY_VERSION=1.2.0
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Install Poetry
RUN apt install curl
RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=$POETRY_VERSION POETRY_HOME=$POETRY_HOME POETRY_VENV=$POETRY_VENV python3 -

# Copies FOCUS validator to the image
COPY . /focus_validator
WORKDIR /focus_validator

# Install FOCUS validator dependencies
RUN /opt/poetry/bin/poetry install

# Sets the entrypoint
ENTRYPOINT [ "/opt/poetry/bin/poetry", "run", "focus-validator" ]
