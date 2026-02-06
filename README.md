# 📘 Marketplace Analytics Pipeline

> An end-to-end data engineering project demonstrating modern data stack practices with synthetic marketplace data

<div align="center">

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

</div>

---

## 🎯 Project Overview

This project demonstrates a complete modern data engineering workflow that simulates a real marketplace environment. The pipeline generates synthetic marketplace data monthly, stores it in Google Cloud Storage, loads it into BigQuery, transforms it using dbt, and orchestrates everything with Apache Airflow—all running in Docker containers with Terraform-managed infrastructure.

**What this pipeline does:**
- 🧪 Generate realistic monthly marketplace data (users, items, searches, purchases, etc.)
- 📦 Store raw data in date-partitioned Google Cloud Storage buckets
- 🗄️ Load data from GCS into BigQuery raw tables
- 🔄 Transform raw data into analytics models using dbt Core
- 🌬️ Orchestrate complete workflow with Apache Airflow DAGs
- 🏗️ Deploy and manage infrastructure using Terraform
- 🐳 Run everything in containerized Docker environment

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Python + Faker** | Generate realistic synthetic marketplace data with proper distributions |
| **Google Cloud Storage** | Data lake for raw CSV files with date partitioning |
| **Google BigQuery** | Cloud data warehouse for scalable storage and analytics |
| **dbt Core** | SQL-based transformations, testing, and documentation |
| **Apache Airflow** | Workflow orchestration with DAG dependencies and scheduling |
| **Terraform** | Infrastructure as Code (IaC) for reproducible cloud resources |
| **Docker & Docker Compose** | Containerization for consistent development and deployment |

---

## 🗺️ Architecture

```mermaid
flowchart TD
    subgraph Monthly_Trigger["🗓️ Monthly Schedule (1st of each month)"]
        TRIGGER["Airflow Scheduler"]
    end

    subgraph DAG1["DAG 1: Data Generation"]
        A1["Generate Monthly CSV Data<br/>(Python + Faker)"]
        A2["Upload to GCS<br/>(raw/YYYY-MM/)"]
    end

    subgraph GCS["☁️ Google Cloud Storage"]
        B1["raw/2025-01/<br/>users.csv<br/>items.csv<br/>..."]
        B2["raw/2025-02/<br/>users.csv<br/>items.csv<br/>..."]
        B3["raw/YYYY-MM/<br/>..."]
    end

    subgraph DAG2["DAG 2: Data Ingestion"]
        C1["Load CSVs from GCS<br/>to BigQuery Raw"]
    end

    subgraph BigQuery["🗄️ Google BigQuery"]
        D1["Raw Dataset<br/>(Source tables)"]
        D2["Analytics Dataset<br/>(Transformed models)"]
    end

    subgraph DAG3["DAG 3: Transformation"]
        E1["dbt deps"]
        E2["dbt run<br/>(staging → analytics)"]
        E3["dbt test<br/>(data quality)"]
    end

    subgraph Infrastructure["🏗️ Terraform"]
        F1["GCS Bucket"]
        F2["BigQuery Datasets"]
        F3["Service Accounts<br/>& IAM Permissions"]
    end

    TRIGGER -->|Triggers| DAG1
    A1 --> A2
    A2 --> B1 & B2 & B3
    
    DAG1 -->|On Success| DAG2
    B1 & B2 & B3 --> C1
    C1 --> D1
    
    DAG2 -->|On Success| DAG3
    D1 --> E1
    E1 --> E2
    E2 --> E3
    E2 --> D2

    F1 -.->|Creates| GCS
    F2 -.->|Creates| BigQuery
    F3 -.->|Configures| DAG1 & DAG2 & DAG3
```

**Data Flow:**
```
Monthly Schedule → Python (Generate Data) → GCS (raw/YYYY-MM/) → BigQuery (raw) → dbt (transform) → BigQuery (analytics)
                                                                                                                ↓
                                                                                                          Airflow Orchestration
```

---

## 📂 Project Structure

```
marketplace-analytics/
│
├── marketplace_dbt/                    # dbt project
│   ├── models/                        # SQL transformation models
│   │   ├── staging/                   # Staging layer (stg_*)
│   │   │   ├── stg_users.sql
│   │   │   ├── stg_items.sql
│   │   │   ├── stg_searches.sql
│   │   │   ├── stg_clicks.sql
│   │   │   └── ...
│   │   └── analytics/                 # Analytics layer
│   │       ├── fct_purchases.sql
│   │       ├── fct_user_engagement.sql
│   │       ├── dim_users.sql
│   │       └── ...
│   ├── snapshots/                     # dbt snapshots for SCD Type 2
│   ├── seeds/                         # Static reference data
│   ├── macros/                        # Reusable SQL macros
│   ├── tests/                         # Data quality tests
│   ├── dbt_project.yml               # dbt configuration
│   └── logs/                          # dbt execution logs
│
├── data_generator_script_and_files/   # Data generation
│   ├── generate_data.py              # Synthetic data generation script
│   └── csv_files/                    # Generated CSV files (gitignored)
│       └── YYYY-MM/                  # Monthly partitions
│
├── orchestration/                     # Airflow setup
│   └── airflow/
│       ├── dags/
│       │   ├── generate_data_load_to_gcs.py       # DAG 1: Generate & Upload
│       │   ├── transfer_gcs_to_raw_database.py    # DAG 2: GCS → BigQuery
│       │   ├── dbt_pipeline.py                    # DAG 3: dbt Transformations
│       │   ├── generate_synthetic_data.py         # Helper: Data generation logic
│       │   └── upload_to_gcs.py                   # Helper: GCS upload utility
│       ├── dbt_profiles/
│       │   └── profiles.yml          # dbt BigQuery connection config
│       ├── docker-compose.yml        # Airflow services orchestration
│       ├── Dockerfile                # Custom Airflow image with dependencies
│       └── requirements.txt          # Python packages for Airflow
│
├── infrastructure/                   # Terraform IaC
│   └── terraform/
│       ├── main.tf                   # Main infrastructure config
│       ├── variables.tf              # Input variables
│       └── outputs.tf                # Output values
│
├── .gitignore                        # Git ignore rules
├── requirements.txt                  # Project dependencies
└── README.md                         # This file
```

---

## 🚀 Getting Started

### Prerequisites

- **Docker** and **Docker Compose** installed
- **Google Cloud Platform** account with billing enabled
- **Terraform** (v1.0+) installed
- **Python 3.8+** (for local data generation)
- **gcloud CLI** (optional, for easier GCP setup)

### 1. Clone the Repository

```bash
git clone https://github.com/BiancaNiemann/marketplace-analytics.git
cd marketplace-analytics
```

### 2. Set Up Google Cloud Credentials

1. Create a service account in [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Grant the following roles:
   - `BigQuery Admin`
   - `Storage Admin`
   - `Service Account User`
3. Download the JSON key file
4. Store it securely on your local machine:

```bash
mkdir -p ~/.keys
mv ~/Downloads/your-service-account-key.json ~/.keys/service-account-key.json
chmod 600 ~/.keys/service-account-key.json
```

### 3. Configure Terraform Variables

Update `infrastructure/terraform/variables.tf` with your GCP project details:

```hcl
variable "project" {
  description = "GCP Project ID"
  default     = "your-gcp-project-id"  # ← Update this
}

variable "credentials" {
  description = "Path to service account key"
  default     = "/path/to/your/service-account-key.json"  # ← Update this
}
```

### 4. Deploy Infrastructure with Terraform

```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

This creates:
- ✅ GCS bucket (`your-project-data-lake`)
- ✅ BigQuery datasets (`raw`, `analytics`)
- ✅ Service account (`dbt-runner`)
- ✅ IAM permissions (BigQuery Data Editor, Job User, Storage Viewer)

### 5. Configure dbt Profiles

Update `orchestration/airflow/dbt_profiles/profiles.yml` with your project details:

```yaml
marketplace_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id        # ← Update this
      dataset: analytics
      threads: 4
      keyfile: /opt/airflow/.dbt/service-account-key.json
      location: EU                         # or US, ASIA, etc.
```

### 6. Start Airflow with Docker

```bash
cd orchestration/airflow
docker-compose up -d --build
```

This starts:
- 🌐 Airflow Webserver (UI)
- 📅 Airflow Scheduler (task execution)
- 🗄️ PostgreSQL (metadata database)

### 7. Create Airflow Admin User

```bash
docker-compose run airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 8. Access Airflow UI

Open your browser and navigate to:
```
http://localhost:8080
```

Login with:
- **Username:** `admin`
- **Password:** `admin`

### 9. Enable and Trigger DAGs

In the Airflow UI:
1. Toggle each DAG to **"On"**
2. Manually trigger `generate_data_load_to_gcs` to test
3. Watch the pipeline cascade through all three DAGs

---

## 🔄 Pipeline Workflow

The pipeline consists of three interconnected Airflow DAGs that run sequentially:

### 📊 DAG 1: `generate_data_load_to_gcs`

**Purpose:** Generate synthetic monthly marketplace data and upload to Google Cloud Storage

**Schedule:** `0 0 1 * *` (Monthly, 1st day at midnight)

**Tasks:**
1. **`generate_monthly_data`** - Runs Python script with Faker to create realistic CSV files
   - Generates 8 CSV files (users, items, searches, impressions, clicks, purchases, notifications, lifecycle_events)
   - All timestamps scoped to current month (prevents data leakage)
   - Output: `/tmp/marketplace_data/YYYY-MM/*.csv`

2. **`upload_to_gcs`** - Uploads CSVs to date-partitioned GCS bucket
   - Destination: `gs://your-project-data-lake/raw/YYYY-MM/`
   - Creates folder structure automatically
   - Enables historical data versioning

**Triggers:** DAG 2 upon successful completion

---

### 🗄️ DAG 2: `transfer_gcs_to_raw_database`

**Purpose:** Load CSV files from GCS into BigQuery raw dataset

**Schedule:** Triggered by DAG 1 (external sensor)

**Tasks:** (Runs in parallel for each table)
- **`load_users`**
- **`load_items`**
- **`load_search_events`**
- **`load_impressions`**
- **`load_clicks`**
- **`load_purchases`**
- **`load_notifications`**
- **`load_lifecycle_events`**

**Configuration:**
- Source: `gs://bucket/raw/{{ execution_date.strftime("%Y-%m") }}/table_name.csv`
- Destination: `project.raw.table_name`
- Write disposition: `WRITE_TRUNCATE` (replace monthly)
- Auto-detect schema from CSV headers

**Triggers:** DAG 3 upon successful completion

---

### 🧠 DAG 3: `dbt_pipeline`

**Purpose:** Transform raw data into analytics-ready models

**Schedule:** Triggered by DAG 2 (external sensor)

**Tasks:**
1. **`dbt_deps`** - Install dbt package dependencies
   ```bash
   dbt deps
   ```

2. **`dbt_run`** - Execute all dbt models (staging → analytics)
   ```bash
   dbt run --profiles-dir /opt/airflow/dbt_profiles
   ```
   - Runs staging models (`stg_*`) first
   - Then builds analytics models (`fct_*`, `dim_*`)
   - Materializes tables/views in BigQuery `analytics` dataset

3. **`dbt_test`** - Run data quality tests
   ```bash
   dbt test --profiles-dir /opt/airflow/dbt_profiles
   ```
   - Validates uniqueness, not-null constraints
   - Checks referential integrity
   - Ensures data quality thresholds

**Dependencies:**
```
dbt_deps → dbt_run → dbt_test
```

---

### 🔗 Complete Task Dependencies

```mermaid
graph LR
    A[DAG 1: generate_data_load_to_gcs] -->|On Success| B[DAG 2: transfer_gcs_to_raw_database]
    B -->|On Success| C[DAG 3: dbt_pipeline]
    
    subgraph DAG1
        A1[generate_monthly_data] --> A2[upload_to_gcs]
    end
    
    subgraph DAG2
        B1[load_users]
        B2[load_items]
        B3[load_searches]
        B4[load_impressions]
        B5[load_clicks]
        B6[load_purchases]
        B7[load_notifications]
        B8[load_lifecycle]
    end
    
    subgraph DAG3
        C1[dbt_deps] --> C2[dbt_run] --> C3[dbt_test]
    end
```

**End-to-End Flow:**
1. **Monthly trigger** → Generate data → Upload to GCS
2. **Load all tables** from GCS to BigQuery raw (parallel)
3. **Transform data** with dbt → Run tests → Analytics dataset ready

---

## 🧪 Data Generation

### Generated Tables

The Python script (`generate_data.py`) creates the following tables with realistic distributions:

| Table | Records | Key Fields |
|-------|---------|------------|
| **users** | 5,000 | `user_id`, `signup_ts`, `country`, `city`, `age_group`, `is_seller`, `account_type`, `signup_channel`, `device_type`, `marketing_opt_in`, `is_verified`, `status` |
| **items** | 8,000 | `item_id`, `seller_id`, `title`, `category`, `price`, `created_ts`, `status` |
| **search_events** | 20,000 | `search_id`, `user_id`, `query`, `timestamp` |
| **impressions** | 60,000 | `impression_id`, `search_id`, `item_id`, `position`, `timestamp` |
| **clicks** | 15,000 | `click_id`, `search_id`, `item_id`, `position`, `click_ts` |
| **purchases** | 5,000 | `purchase_id`, `item_id`, `buyer_id`, `price`, `purchase_ts` |
| **notifications** | 12,000 | `notification_id`, `user_id`, `type`, `sent_ts`, `opened` |
| **lifecycle_events** | 5,000 | `event_id`, `user_id`, `event_type`, `timestamp` |

### Sample Data Characteristics

- **User Demographics:** Realistic age distributions, country codes, device types
- **Marketplace Activity:** Follows conversion funnel (searches → impressions → clicks → purchases)
- **Temporal Patterns:** All events scoped to current month with realistic time progression
- **Business Logic:** Click-through rates, purchase conversion rates match industry benchmarks

### Run Data Generation Manually (Optional)

```bash
cd data_generator_script_and_files
python generate_data.py --output_dir csv_files/2025-02
```

---

## 🧠 dbt Models

### Staging Layer (`models/staging/`)

Clean, rename, and standardize raw data with consistent naming conventions:

- **`stg_users.sql`** - Standardize user attributes, parse timestamps
- **`stg_items.sql`** - Clean item data, categorize products
- **`stg_searches.sql`** - Normalize search queries
- **`stg_impressions.sql`** - Link impressions to searches and items
- **`stg_clicks.sql`** - Track click behavior with position analysis
- **`stg_purchases.sql`** - Transaction details with revenue calculations
- **`stg_notifications.sql`** - Communication events with engagement metrics
- **`stg_lifecycle_events.sql`** - User journey milestones

### Analytics Layer (`models/analytics/`)

Business logic and aggregated metrics:

**Fact Tables:**
- **`fct_purchases.sql`** - Complete purchase transactions with user/item context
- **`fct_user_engagement.sql`** - Daily user activity metrics (searches, clicks, purchases)
- **`fct_conversion_funnel.sql`** - Search → Impression → Click → Purchase funnel

**Dimension Tables:**
- **`dim_users.sql`** - User attributes with acquisition channel
- **`dim_items.sql`** - Product catalog with category hierarchies
- **`dim_dates.sql`** - Date dimension for time-based analysis

**Aggregate Tables:**
- **`agg_daily_metrics.sql`** - Daily KPIs (revenue, orders, active users)
- **`agg_seller_performance.sql`** - Seller-level metrics
- **`agg_category_trends.sql`** - Product category performance

### dbt Tests

Data quality tests ensure:
- ✅ Unique primary keys
- ✅ Not-null constraints on critical fields
- ✅ Referential integrity (foreign keys exist)
- ✅ Accepted value ranges (e.g., prices > 0)
- ✅ Row count thresholds

### Run dbt Locally (Optional)

```bash
cd marketplace_dbt

# Install dependencies
dbt deps

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # Opens in browser at http://localhost:8080
```

---

## 🐳 Docker Services

The `docker-compose.yml` orchestrates the following services:

| Service | Purpose | Port | Health Check |
|---------|---------|------|--------------|
| **postgres** | Airflow metadata database | 5432 | `pg_isready` |
| **airflow-webserver** | Airflow UI & REST API | 8080 | HTTP probe |
| **airflow-scheduler** | DAG scheduling & task execution | - | Process monitor |

### Environment Variables

Key configurations in `docker-compose.yml`:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/.dbt/service-account-key.json
```

### Useful Docker Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View real-time logs
docker-compose logs -f airflow-scheduler

# Rebuild containers after code changes
docker-compose up -d --build

# Execute commands inside containers
docker-compose exec airflow-webserver bash

# Restart specific service
docker-compose restart airflow-scheduler

# Remove all containers and volumes (fresh start)
docker-compose down -v
```

---

## 🧱 Infrastructure as Code

### Terraform Resources

The `main.tf` provisions:

1. **Google Cloud Storage Bucket**
   ```hcl
   resource "google_storage_bucket" "data_lake" {
     name          = "${var.project}-data-lake"
     location      = var.region
     storage_class = "STANDARD"
     versioning { enabled = true }
   }
   ```

2. **BigQuery Datasets**
   ```hcl
   resource "google_bigquery_dataset" "raw" {
     dataset_id = "raw"
     location   = "EU"
   }
   
   resource "google_bigquery_dataset" "analytics" {
     dataset_id = "analytics"
     location   = "EU"
   }
   ```

3. **Service Account & IAM**
   ```hcl
   resource "google_service_account" "dbt" {
     account_id   = "dbt-runner"
     display_name = "dbt Runner Service Account"
   }
   
   resource "google_project_iam_member" "dbt_bigquery_roles" {
     for_each = toset([
       "roles/bigquery.dataEditor",
       "roles/bigquery.jobUser",
       "roles/storage.objectViewer"
     ])
     role   = each.key
     member = "serviceAccount:${google_service_account.dbt.email}"
   }
   ```

### Terraform Commands

```bash
cd infrastructure/terraform

# Initialize backend and download providers
terraform init

# Validate configuration syntax
terraform validate

# Preview changes before applying
terraform plan

# Apply infrastructure changes
terraform apply

# Destroy all resources (use with caution!)
terraform destroy

# Format code to HCL standards
terraform fmt

# Show current state
terraform show
```

---

## 📊 Monitoring and Logs

### Airflow Monitoring

**Airflow UI:** http://localhost:8080

Features:
- 📊 DAG run history and success rates
- ⏱️ Task duration metrics
- 🔴 Failed task alerts with logs
- 📅 Gantt charts for task execution
- 📈 Landing times for schedule adherence

**Logs Location:**
- Container: `/opt/airflow/logs/`
- Local mount: `orchestration/airflow/logs/`

**View logs:**
```bash
# Real-time scheduler logs
docker-compose logs -f airflow-scheduler

# Specific task logs
docker-compose exec airflow-webserver cat /opt/airflow/logs/dag_id/task_id/YYYY-MM-DD/1.log
```

### dbt Logs

**Location:** `marketplace_dbt/logs/dbt.log`

**Generated after:**
- `dbt run` - Model compilation and execution
- `dbt test` - Test results
- `dbt docs generate` - Documentation build

**View logs:**
```bash
tail -f marketplace_dbt/logs/dbt.log
```

### Google Cloud Monitoring

**BigQuery:**
- Query history: [BigQuery Console](https://console.cloud.google.com/bigquery)
- Slot usage and costs
- Table/dataset metadata

**Cloud Storage:**
- Bucket size and object counts
- Access logs (if enabled)

**Cost Monitoring:**
- [Billing Reports](https://console.cloud.google.com/billing)
- Set budget alerts for unexpected costs

---

## 🔒 Security Best Practices

### Implemented Security Measures

- ✅ **Service Account Key Storage** - Stored outside repository in `~/.keys/`
- ✅ **Least Privilege IAM** - Minimal required permissions only
- ✅ **Gitignore Protection** - Credentials, logs, and data excluded from Git
- ✅ **Environment Variables** - Sensitive config via Docker env vars
- ✅ **GCS Versioning** - Enabled for data recovery and audit trails

### Recommendations

- 🔐 **Rotate keys regularly** - Update service account keys every 90 days
- 🔐 **Use Workload Identity** - For production GKE deployments (eliminates keys)
- 🔐 **Enable audit logging** - Track all BigQuery and GCS access
- 🔐 **Restrict network access** - Use VPC Service Controls in production
- 🔐 **Secrets management** - Consider HashiCorp Vault or GCP Secret Manager

### What NOT to Commit

```bash
# Already in .gitignore
*.json          # Service account keys
*.csv           # Generated data files
*.log           # Application logs
.env            # Environment variables
__pycache__/    # Python cache
```

---

## 🐛 Troubleshooting

### Common Issues and Solutions

#### 1. **Service Account Key Not Found**

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/.dbt/service-account-key.json'
```

**Solution:**
- Verify key location: `~/.keys/service-account-key.json`
- Check `docker-compose.yml` volume mount:
  ```yaml
  volumes:
    - ~/.keys/service-account-key.json:/opt/airflow/.dbt/service-account-key.json:ro
  ```
- Ensure file permissions: `chmod 600 ~/.keys/service-account-key.json`

---

#### 2. **Airflow Scheduler Not Picking Up DAGs**

**Error:** DAGs not visible in UI

**Solution:**
```bash
# Check DAG parsing errors
docker-compose logs airflow-scheduler | grep -i error

# Verify DAG file syntax
docker-compose exec airflow-webserver python /opt/airflow/dags/your_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

---

#### 3. **BigQuery Permission Denied**

**Error:**
```
403 Forbidden: Access Denied: Project your-project: User does not have permission
```

**Solution:**
- Grant required IAM roles via Terraform or GCP Console:
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`
- Wait 60-120 seconds for IAM propagation
- Test with `gcloud auth activate-service-account`

---

#### 4. **dbt Connection Errors**

**Error:**
```
Database Error in model staging.stg_users
  Invalid project ID 'None'
```

**Solution:**
- Validate `dbt_profiles/profiles.yml`:
  - Check `project:` matches your GCP project ID
  - Verify `keyfile:` path is correct
  - Ensure `dataset:` exists in BigQuery

---

#### 5. **Docker Port Already in Use**

**Error:**
```
Error starting userland proxy: listen tcp4 0.0.0.0:8080: bind: address already in use
```

**Solution:**
```bash
# Find process using port 8080
lsof -i :8080

# Kill the process or change port in docker-compose.yml
ports:
  - "8081:8080"  # Map to different host port
```

---

#### 6. **GCS Upload Fails**

**Error:**
```
google.api_core.exceptions.PermissionDenied: 403 Caller does not have storage.objects.create access
```

**Solution:**
- Add `roles/storage.objectAdmin` to service account
- Verify bucket name in DAG matches Terraform output
- Check bucket exists: `gsutil ls`

---

#### 7. **DAG Import Errors**

**Error:**
```
Broken DAG: No module named 'google.cloud.storage'
```

**Solution:**
- Add package to `orchestration/airflow/requirements.txt`
- Rebuild Docker image:
  ```bash
  docker-compose down
  docker-compose up -d --build
  ```

---

## 🚀 Future Enhancements

Potential improvements for this project:

- [ ] **Incremental Models** - Implement dbt incremental models for large fact tables
- [ ] **Data Quality Dashboard** - Visualize dbt test results in Looker/Tableau
- [ ] **CI/CD Pipeline** - GitHub Actions for automated testing and deployment
- [ ] **dbt Cloud Integration** - Migrate to dbt Cloud for easier collaboration
- [ ] **Real-Time Streaming** - Add Pub/Sub → Dataflow for real-time events
- [ ] **Cost Optimization** - Implement BigQuery partitioning and clustering
- [ ] **Alerting** - Slack/email notifications for DAG failures
- [ ] **Data Cataloging** - Integrate with Google Data Catalog or Atlan
- [ ] **Multi-Environment** - Separate dev/staging/prod environments

---

## 📝 License

This project is open source and available under the MIT License for educational purposes.

---

## 📧 Contact

**Bianca Niemann**

[![GitHub](https://img.shields.io/badge/GitHub-BiancaNiemann-181717?style=flat&logo=github)](https://github.com/BiancaNiemann)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://www.linkedin.com/in/bianca-niemann)

---

## 🙏 Acknowledgments

This project was built using industry-standard tools and best practices:

- **[Faker](https://faker.readthedocs.io/)** - Synthetic data generation
- **[dbt Labs](https://www.getdbt.com/)** - Analytics engineering framework
- **[Apache Airflow](https://airflow.apache.org/)** - Workflow orchestration
- **[Google Cloud Platform](https://cloud.google.com/)** - Cloud infrastructure
- **[Terraform](https://www.terraform.io/)** - Infrastructure as Code
- **[Docker](https://www.docker.com/)** - Containerization platform

Special thanks to the data engineering community for open-source tools and documentation.

---

<div align="center">

⭐ **Star this repo** if you find it helpful!

**Made with ❤️ by Bianca Niemann**

</div>
