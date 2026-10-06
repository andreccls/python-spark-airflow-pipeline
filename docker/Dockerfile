# One image for everything: Airflow 2.10 (official) + Java 17 + PySpark.
#  - target "runtime": used by the Airflow services
#  - target "dev":     adds pytest / coverage / ruff / mypy (used by make test|lint|coverage)
FROM apache/airflow:2.10.5-python3.12 AS runtime

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

FROM runtime AS dev
COPY docker/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
