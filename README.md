# S3 to Redshift Data Pipeline

An automated ETL pipeline that loads CSV files from S3 into Redshift using Airflow. Built with Terraform, tested with pytest, and deployed via GitHub Actions.

## What It Does

Loads sales order data from S3 → Redshift Staging → Quality checks → Redshift Curated

The pipeline handles:
- CSV ingestion via Redshift COPY command
- Null checks and deduplication
- Staging/curated architecture for rollback safety
- Automated testing and CI

## Quick Start

**Prerequisites:** AWS account, Python 3.12+, Docker

```bash
# Setup Python environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r airflow/requirements.txt

# Initialize Airflow for tests
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate

# Run tests
pytest airflow/tests -v

# Deploy infrastructure
cd infra/terraform
terraform init
terraform apply -var="region=eu-west-2"

# Start Airflow
cd ../../airflow
docker compose up -d
```

Access Airflow at http://localhost:8080

## Project Structure

```
airflow/
  dags/
    s3_to_redshift_pipeline.py  # Main ETL DAG
    test.py                      # Connection test
  tests/
    test_dag_structure.py        # DAG validation tests
  docker-compose.yaml
  Dockerfile
  requirements.txt

infra/terraform/               # AWS infrastructure
  s3.tf                        # Raw data bucket
  redshift.tf                  # Serverless cluster
  iam.tf                       # S3 access roles
  
sql/                           # Schema definitions
  00_schema.sql               # Create schemas
  01_staging_ddl.sql          # Staging table
  02_curated_ddl.sql          # Curated table
  03_quality.sql              # Quality queries

data/sample/
  sales_orders.csv            # Sample data
```

## Configuration

After running terraform, update the DAG file with your values:

Edit `airflow/dags/s3_to_redshift_pipeline.py` line ~48:

```python
COPY staging.sales_orders
FROM 's3://YOUR-BUCKET-NAME/raw/sales_orders/'
IAM_ROLE 'YOUR-IAM-ROLE-ARN'
```

Get these from terraform outputs:
```bash
cd infra/terraform
terraform output
```

Set up Airflow connection `redshift_default`:
- Type: Postgres
- Host: your-redshift-endpoint.amazonaws.com
- Port: 5439
- Schema: analytics
- User/Password: from AWS console

## DAG Flow

```
load_staging → validate_staging → promote_curated → final_quality_check
```

Tasks:
1. `load_staging` - COPY from S3 to staging.sales_orders
2. `validate_staging` - Check nulls, row counts
3. `promote_curated` - Deduplicate and move to curated schema
4. `final_quality_check` - Validate no duplicates in final table

## Testing

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
pytest airflow/tests -v
```

Tests verify:
- DAG loads without import errors
- All 4 tasks are present
- Task dependencies are correct

Linting:
```bash
flake8 airflow/dags --max-line-length=120
```

## CI/CD

GitHub Actions runs on every push:
- Lints Python code
- Validates DAG imports
- Runs pytest suite

## Power BI Dashboard

Skipped - developed on Linux where Power BI Desktop isn't available. The data pipeline is complete and ready to connect from any BI tool.

If connecting from Windows:
1. Power BI Desktop → Get Data → PostgreSQL
2. Use Redshift endpoint as server
3. Connect to `curated.sales_orders` table

## Troubleshooting

**DAG import errors:** Check Airflow logs, verify connection ID exists

**COPY command fails:** Query `stl_load_errors` in Redshift for details

**Tests fail with "no such table":** Run `airflow db migrate` first

**Connection timeout:** Check Redshift security group allows your IP on port 5439

## Cleanup

```bash
# Stop Airflow
docker compose down

# Destroy AWS resources
cd infra/terraform
terraform destroy
```

## What's Next

Some improvements for production:
- Add secrets management (AWS Secrets Manager)
- Implement incremental loads instead of full truncate/load
- Add monitoring and alerting
- Set up scheduled DAG runs
- Add more comprehensive data quality rules
