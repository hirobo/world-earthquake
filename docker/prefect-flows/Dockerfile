FROM prefecthq/prefect:2.8.4-python3.9

COPY docker/prefect-flows/requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY prefect/flows /opt/prefect/flows
COPY dbt /opt/dbt

RUN mkdir -p /opt/prefect/data
