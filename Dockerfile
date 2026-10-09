# The app image: generator, consumers, reconciler. It runs on the Compose network because the
# emulated MSK and the Postgres replication port are not reachable from the host.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv PATH=/opt/venv/bin:$PATH
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev
# DuckDB's S3 and Iceberg readers, installed now so a Lambda needs neither a network nor a writable home.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute(\"set extension_directory='/opt/duckdb'\"); c.execute('install httpfs'); c.execute('install iceberg')"
COPY dbt ./dbt
ENTRYPOINT ["lossproof"]
