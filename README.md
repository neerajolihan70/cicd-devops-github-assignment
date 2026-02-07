# S3 to Redshift Data Pipeline

[![CI Pipeline](https://github.com/yourusername/github-assignment/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/github-assignment/actions)

An automated ETL pipeline that loads CSV files from S3 into Redshift Serverless using Airflow orchestration. Includes data quality checks, automated testing, and CI/CD.

---

## 🏗️ Architecture Overview

```
┌─────────────┐     ┌──────────────┐     ┌───────────────────┐
│  CSV Files  │────▶│ S3 Raw Bucket│────▶│ Redshift Staging  │
└─────────────┘     └──────────────┘     └───────────────────┘
                                                   │
                           ┌───────────────────────┘
                           │
                           ▼
                    ┌──────────────┐     ┌──────────────────┐
                    │ Data Quality │────▶│ Redshift Curated │
                    │    Checks    │     └──────────────────┘
                    └──────────────┘              │
                                                  │
        ┌──────────────┐                         │
        │ GitHub       │                         ▼
        │ Actions CI   │                  ┌─────────────┐
        └──────────────┘                  │  Power BI   │
               │                           │  Dashboard  │
               ▼                           └─────────────┘
        ┌──────────────┐
        │   Airflow    │──────────────────▶ Orchestration
        │     DAG      │
        └──────────────┘
               │
               ▼
        ┌──────────────┐
        │  Terraform   │──────────────────▶ Infrastructure
        └──────────────┘
```

---

## 🚀 Quick Start

**Prerequisites:** AWS account, Python 3.12+, Docker, Terraform

### Run It in 5 Steps

```bash
# 1. Initialize Python environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r airflow/requirements.txt

# 2. Initialize Airflow DB for tests
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate

# 3. Run tests (ALL PASSING ✅)
pytest airflow/tests -v
# ✅ test_dag_loaded PASSED
# ✅ test_dag_has_correct_tasks PASSED
# ✅ test_dag_dependencies PASSED

# 4. Provision infrastructure
cd infra/terraform
terraform init
terraform apply -var="region=eu-west-2" -auto-approve

# 5. Start Airflow
cd ../../airflow
docker compose up -d
```

---

## ✨ Features Implemented

### ✅ Data Pipeline (COMPLETE)
- S3 to Redshift loading via COPY command
- Staging → Curated pattern
- 4What's Included

**Data Pipeline**
- 4-task Airflow DAG (load → validate → curate → quality check)
- S3 to Redshift via COPY command
- Staging/curated architecture for safe rollback

**Data Quality**
- Null checks, duplicate detection, schema validation
- Automated quality gates between stages

**Infrastructure**
- Terraform-managed S3, Redshift Serverless, IAM roles
- Deployed to EU-West-2

**Testing & CI/CD**
- 3 pytest tests (all passing)
- GitHub Actions with linting and validation
- Zero flake8 violations
**See [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) for detailed step-by-step instructions.**

Key sections:
1. AWS Infrastructure setup with Terraform
2. How It Works

1. **Terraform** provisions S3 bucket, Redshift cluster, and IAM roles
2. **Sample CSV** gets uploaded to S3
3. **Airflow DAG** triggers and:
   - Loads data to staging table
   - Runs quality checks
   - Promotes clean data to curated table
   - Validates final output
4. **GitHub Actions** runs tests on every push

See [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md) for detailed walkthrough.

```bash
# Initialize Airflow database
export AIRFLOW_HOME=$(pwd)/airflow
airTesting

```bash
# Setup
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate

# Run tests (3/3 passing)
pytest airflow/tests -v

# Lint check (0 violations)
flake8 airflow/dags --max-line-length=120

```
load_staging
    ↓
validate_staging
    ↓
promote_curated
    ↓
final_quality_check
```DAG Tasks

```
load_staging          → Load CSV from S3 to staging table
    ↓
validate_staging      → Check nulls, duplicates, schema
    ↓
promote_curated       → Dedupe and insert to curated table
    ↓
final_quality_check   → Validate final data quality
```
│   │   └── test_dag_structure.py       # 3 passing tests
│   └── requirements.txt
├── infra/terraform/               # AWS infrastructure
├── sql/                           # DDL and quality checks
├── powerbi/                       # Dashboard files
├── data/sample/                   # Sample CSV
├── docs/                          # Screenshots & diagrams
├── README.md                      # This file
└── IMPLEMENTATION_GUIDE.md        # Detailed guide
```

---

## 🔧 Configuration

### Update DAG with Your Values

Edit `airflow/dags/s3_to_redshift_pipeline.py`:

```python
# Line ~48: Update S3 bucket and IAM role
COPY staging.sales_orders
FROM 's3://YOUR-BUCKET-NAME/raw/sales_orders/'
IAM_ROLE 'YOUR-IAM-ROLE-ARN'
```

### Airflow Connection

Configure `redshift_default` connection:
- **Type**: Postgres
- **Host**: Your Redshift endpoint
- **Port**: 5439
- *Configuration

**Update** `airflow/dags/s3_to_redshift_pipeline.py` with your S3 bucket and IAM role ARN.

**Airflow Connection** `redshift_default`:
- Type: Postgres
- Host: `your-redshift-endpoint.amazonaws.com`
- Port: 5439
- Schema: analytics
- Login/Password: admin

## 🐛 Troubleshooting

### Tests Fail with "no such table: dag"

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
pytest airflow/tests -v
```

### Airflow Connection Issues

- CWhat's Delivered

✅ **Infrastructure** - Terraform configs for S3, Redshift, IAM  
✅ **Airflow DAG** - 4-task pipeline with quality checks  
✅ **Data Quality** - Null checks, duplicate detection, validation  
✅ **Rollback** - Staging table isolation for safe recovery  
✅ **Error Handling** - Try/except blocks, detailed logging  
✅ **CI/CD** - GitHub Actions with lint/test automation  
✅ **Tests** - 3/3 passing, 0 linting violations  
✅ **Documentation** - Complete guides and troubleshootingLESHOOTING.md](docs/TROUBLESHOOTING.md)**

---

## 🧹 Cleanup

```bash
# Stop Airflow
cd Common Issues

**Tests fail:** Run `airflow db migrate` first  
**Connection errors:** Check Redshift security group allows port 5439  
**COPY failures:** Query `stl_load_errors` table in Redshift

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for detailed solutions.
3. **Error Handling**: How does rollback work?
4. **Idempotency**: Can you rerun safely?
5. **Scalability**: How to handle larger data?
6. **Monitoring**: How to detect failures?
7. **Testing**: What's tested and why?

---

**📖 For complete implementation details, see [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)**  
**🔧 For troubleshooting help, see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**

**Tests are passing ✅ | Ready for deployment 🚀**
Documentation

**Start here:**
- [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md) - Quick overview of what this does
- [docs/COMPLETION_STATUS.md](docs/COMPLETION_STATUS.md) - What's done, what's not

**Reference:**
- [docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md) - Common commands and queries
- [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md) - Step-by-step guide
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - Solutions to common issues
- [docs/architecture.md](docs/architecture.md) - Design decisions

---

**Status:** All tests passing ✅ Ready for deployment 🚀# cicd-devops-github-assignment
