FROM python:3.13 as python-base

# Set Poetry's configs
ENV POETRY_VERSION=2.2.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV PATH="${PATH}:${POETRY_VENV}/bin"
ENV POETRY=${POETRY_VENV}/bin/poetry
ENV WORKING_DIR=/focus_validator

# Install Poetry
RUN apt install curl
RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=$POETRY_VERSION POETRY_HOME=$POETRY_HOME POETRY_VENV=$POETRY_VENV python3 -

# Create working directory
RUN mkdir ${WORKING_DIR}
WORKDIR ${WORKING_DIR}

# Copies poetry config to the image and Install FOCUS validator dependencies only
COPY README.md README.md
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml
RUN /opt/poetry/bin/poetry install --only main --no-root

# Copies FOCUS validator to the image
COPY . ${WORKING_DIR}

# Install the focus_validator package itself
RUN /opt/poetry/bin/poetry install --only-root

# Sets the entrypoint
ENTRYPOINT [ "${POETRY}", "run", "focus-validator" ]
