"""
S3 to Redshift Data Pipeline DAG

This DAG:
1. Loads CSV data from S3 into Redshift staging schema
2. Validates data quality
3. Promotes clean data to curated schema
4. Runs final quality checks
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging

# Get logger
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_staging(**context):
    """Load data from S3 into staging table with error handling"""
    try:
        hook = PostgresHook(postgres_conn_id="redshift_default")

        logger.info("Starting S3 to Redshift COPY operation")

        sql = """
        BEGIN;

        -- Truncate staging table
        TRUNCATE TABLE staging.sales_orders;

        -- Load from S3
        COPY staging.sales_orders
        FROM 's3://github-assignment-raw-cf3e4408/raw/sales_orders/'
        IAM_ROLE 'arn:aws:iam::080617914168:role/github-assignment-redshift-role'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'YYYY-MM-DD'
        TIMEFORMAT 'auto'
        BLANKSASNULL
        EMPTYASNULL
        MAXERROR 10;

        COMMIT;
        """

        hook.run(sql)

        # Get row count
        result = hook.get_first("SELECT COUNT(*) FROM staging.sales_orders")
        row_count = result[0] if result else 0

        logger.info(
            f"Successfully loaded {row_count} rows into staging.sales_orders")

        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(
            key='staging_row_count', value=row_count)

        if row_count == 0:
            raise AirflowException("No data loaded into staging table")

    except Exception as e:
        logger.error(f"Error loading staging data: {str(e)}")
        raise


def validate_staging(**context):
    """Run data quality checks on staging data"""
    try:
        hook = PostgresHook(postgres_conn_id="redshift_default")

        logger.info("Running staging data quality checks")

        # Check for nulls in critical columns
        null_check_sql = """
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
            SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_dates,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
        FROM staging.sales_orders
        """

        result = hook.get_first(null_check_sql)
        total, null_ids, null_dates, null_customers, null_amounts = result

        logger.info(f"Staging validation: {total} total rows")
        logger.info(
            f"Null order_ids: {null_ids}, dates: {null_dates}, customers: {null_customers}, amounts: {null_amounts}")

        # Warning if more than 10% nulls
        if null_ids > total * 0.1:
            logger.warning(f"High null rate in order_id: {null_ids}/{total}")

    except Exception as e:
        logger.error(f"Error validating staging data: {str(e)}")
        raise


def promote_curated(**context):
    """Promote validated data from staging to curated with transformation"""
    try:
        hook = PostgresHook(postgres_conn_id="redshift_default")

        logger.info("Promoting data from staging to curated")

        sql = """
        BEGIN;

        -- Create temp backup table
        CREATE TEMP TABLE curated_backup AS
        SELECT * FROM curated.sales_orders;

        -- Truncate curated table
        TRUNCATE TABLE curated.sales_orders;

        -- Insert clean data with deduplication
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

        hook.run(sql)

        # Get row count
        result = hook.get_first("SELECT COUNT(*) FROM curated.sales_orders")
        row_count = result[0] if result else 0

        logger.info(
            f"Successfully promoted {row_count} rows to curated.sales_orders")

        context['task_instance'].xcom_push(
            key='curated_row_count', value=row_count)

    except Exception as e:
        logger.error(f"Error promoting to curated: {str(e)}")
        # Rollback would happen automatically
        raise


def final_quality_check(**context):
    """Run final quality checks on curated data"""
    try:
        hook = PostgresHook(postgres_conn_id="redshift_default")

        logger.info("Running final quality checks")

        # Duplicate check
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
            raise AirflowException(
                f"Found {duplicates} duplicate order_ids in curated table")

        # Referential integrity check (all customers exist)
        # In real scenario, you'd check against customer dimension table

        logger.info("All quality checks passed!")

    except Exception as e:
        logger.error(f"Quality check failed: {str(e)}")
        raise


# Define DAG
with DAG(
    dag_id="s3_to_redshift_pipeline",
    default_args=default_args,
    description='Load data from S3 to Redshift with quality checks',
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'redshift', 'etl'],
) as dag:

    load = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
        provide_context=True,
    )

    curate = PythonOperator(
        task_id="promote_curated",
        python_callable=promote_curated,
        provide_context=True,
    )

    quality = PythonOperator(
        task_id="final_quality_check",
        python_callable=final_quality_check,
        provide_context=True,
    )

    load >> validate >> curate >> quality
