FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# COPY --from=docker.io/astral/uv:latest /uv /bin/

# RUN pip install pandas pyarrow
WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_pipeline.py .

# docker build -t test_pipeline:v2 . 
# docker run -it --entrypoint=bash --rm test_pipeline:v2
ENTRYPOINT [ "python", "ingest_pipeline.py" ]

# docker run -it  --rm test_pipeline:v2