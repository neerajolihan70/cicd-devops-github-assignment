"""
S3 to Redshift ETL Pipeline

Loads sales order CSV from S3, validates data quality, and promotes to curated schema.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_staging(**context):
    """Load CSV data from S3 into staging table"""
    hook = PostgresHook(postgres_conn_id="redshift_default")
    
    logger.info("Starting S3 COPY operation")
    
    # TODO: parameterize bucket name and role ARN
    sql = """
    BEGIN;
    
    TRUNCATE TABLE staging.sales_orders;
    
    COPY staging.sales_orders
    FROM 's3://github-assignment-raw-cf3e4408/raw/sales_orders/'
    IAM_ROLE 'arn:aws:iam::080617914168:role/github-assignment-redshift-role'
    CSV
    IGNOREHEADER 1
    DATEFORMAT 'YYYY-MM-DD'
    BLANKSASNULL
    EMPTYASNULL
    MAXERROR 10;
    
    COMMIT;
    """
    
    try:
        hook.run(sql)
        result = hook.get_first("SELECT COUNT(*) FROM staging.sales_orders")
        row_count = result[0] if result else 0
        
        logger.info(f"Loaded {row_count} rows into staging")
        context['task_instance'].xcom_push(key='staging_row_count', value=row_count)
        
        if row_count == 0:
            raise AirflowException("No data loaded - check S3 path and IAM permissions")
            
    except Exception as e:
        logger.error(f"Load failed: {str(e)}")
        raise


def validate_staging(**context):
    """Check for nulls and basic data quality issues"""
    hook = PostgresHook(postgres_conn_id="redshift_default")
    
    logger.info("Running staging validation")
    
    null_check = """
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_orders,
        SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_dates,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
    FROM staging.sales_orders
    """
    
    result = hook.get_first(null_check)
    total, null_orders, null_dates, null_customers, null_amounts = result
    
    logger.info(f"Validation: {total} rows, nulls - orders:{null_orders}, dates:{null_dates}, customers:{null_customers}, amounts:{null_amounts}")
    
    # Warn if >10% nulls in critical columns
    if null_orders > total * 0.1:
        logger.warning(f"High null rate in order_id: {null_orders}/{total}")


def promote_curated(**context):
    """Move validated data to curated with deduplication"""
    hook = PostgresHook(postgres_conn_id="redshift_default")
    
    logger.info("Promoting to curated schema")
    
    sql = """
    BEGIN;
    
    -- backup current curated data
    CREATE TEMP TABLE curated_backup AS
    SELECT * FROM curated.sales_orders;
    
    TRUNCATE TABLE curated.sales_orders;
    
    -- insert deduplicated clean data
    INSERT INTO curated.sales_orders
    SELECT
        order_id,
        order_date,
        customer_id,
        amount
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) as rn
        FROM staging.sales_orders
        WHERE order_id IS NOT NULL
            AND order_date IS NOT NULL
            AND customer_id IS NOT NULL
            AND amount > 0
    ) t
    WHERE rn = 1;
    
    COMMIT;
    """
    
    try:
        hook.run(sql)
        result = hook.get_first("SELECT COUNT(*) FROM curated.sales_orders")
        row_count = result[0] if result else 0
        
        logger.info(f"Promoted {row_count} rows to curated")
        context['task_instance'].xcom_push(key='curated_row_count', value=row_count)
        
    except Exception as e:
        logger.error(f"Promotion failed: {str(e)}")
        raise


def final_quality_check(**context):
    """Verify no duplicates in final curated table"""
    hook = PostgresHook(postgres_conn_id="redshift_default")
    
    logger.info("Running final QA checks")
    
    duplicate_check = """
    SELECT COUNT(*)
    FROM (
        SELECT order_id, COUNT(*) as cnt
        FROM curated.sales_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1
    ) dupes
    """
    
    result = hook.get_first(duplicate_check)
    duplicates = result[0] if result else 0
    
    if duplicates > 0:
        raise AirflowException(f"Found {duplicates} duplicate order_ids in curated")
    
    logger.info("Quality checks passed")


with DAG(
    dag_id="s3_to_redshift_pipeline",
    default_args=default_args,
    description='S3 to Redshift ETL with quality checks',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'redshift', 'etl'],
) as dag:
    
    load = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
    )
    
    validate = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )
    
    curate = PythonOperator(
        task_id="promote_curated",
        python_callable=promote_curated,
    )
    
    quality = PythonOperator(
        task_id="final_quality_check",
        python_callable=final_quality_check,
    )
    
    load >> validate >> curate >> quality
