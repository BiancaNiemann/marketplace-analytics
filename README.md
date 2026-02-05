# 📘 Marketplace Analytics Pipeline

A beginner-friendly guide to building an end-to-end data engineering project using modern tools and best practices.

## 🎯 Project Overview

This project demonstrates a complete modern data engineering workflow that simulates a real marketplace environment. The pipeline generates synthetic marketplace data, stores it in Google BigQuery, transforms it using dbt, and orchestrates everything with Apache Airflow—all running in Docker containers.

**What this pipeline does:**
- 🧪 Generate fake marketplace data (clicks, orders, products, etc.)
- 📦 Store raw data in BigQuery
- 🔄 Transform data using dbt Core
- 🌬️ Orchestrate workflows with Airflow
- 🏗️ Deploy infrastructure using Terraform
- 🐳 Run everything inside Docker containers

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Python + Faker** | Generate realistic synthetic marketplace data |
| **Google BigQuery** | Cloud data warehouse for storage and analytics |
| **dbt Core** | SQL-based data transformations and modeling |
| **Apache Airflow** | Workflow orchestration and scheduling |
| **Terraform** | Infrastructure as Code (IaC) for cloud resources |
| **Docker** | Containerization for consistent environments |

---

## 🗺️ Architecture



**Data Flow:**
```
Python + Faker → CSV files → BigQuery (raw) → dbt (transform) → BigQuery (analytics) → Airflow (orchestration)
```

---

## 📂 Project Structure

```
marketplace-analytics/
│
├── marketplace_dbt/                    # dbt project
│   ├── models/                        # SQL transformation models
│   │   ├── staging/                   # Staging layer (stg_*)
│   │   └── analytics/                 # Analytics layer
│   ├── snapshots/                     # dbt snapshots
│   ├── seeds/                         # Static CSV data
│   ├── macros/                        # Reusable SQL macros
│   ├── tests/                         # Data quality tests
│   ├── dbt_project.yml               # dbt configuration
│   └── logs/                          # dbt logs
│
├── data_generator_script_and_files/   # Data generation
│   ├── generate_data.py              # Python Faker scripts
│   └── *.csv                          # Generated CSV files
│
├── orchestration/                     # Airflow setup
│   └── airflow/
│       ├── dags/
│       │   └── dbt_pipeline.py       # Main DAG
│       ├── dbt_profiles/
│       │   └── profiles.yml          # dbt BigQuery connection
│       ├── docker-compose.yml        # Airflow services
│       └── Dockerfile                # Custom Airflow image
│
├── infrastructure/                    # Terraform IaC
│   └── terraform/
│       ├── main.tf                   # Main infrastructure config
│       ├── variables.tf              # Input variables
│       └── outputs.tf                # Output values
│
├── .gitignore                        # Git ignore rules
└── README.md                         # This file
```

---

## 🚀 Getting Started

### Prerequisites

- **Docker** and **Docker Compose** installed
- **Google Cloud Platform** account
- **Terraform** installed (for infrastructure setup)
- **Python 3.8+** (for data generation)

### 1. Clone the Repository

```bash
git clone https://github.com/BiancaNiemann/marketplace-analytics.git
cd marketplace-analytics
```

### 2. Set Up Google Cloud Credentials

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Store it securely on your local machine:

```bash
mkdir -p ~/.keys
mv ~/Downloads/your-service-account-key.json ~/.keys/service-account-key.json
chmod 600 ~/.keys/service-account-key.json
```

### 3. Configure dbt Profiles

Update `orchestration/airflow/dbt_profiles/profiles.yml` with your project details:

```yaml
marketplace_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id        # Update this
      dataset: analytics                   # Update if needed
      threads: 4
      keyfile: /opt/airflow/.dbt/service-account-key.json
      location: EU                         # or US
```

### 4. Deploy Infrastructure with Terraform

```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

This creates:
- BigQuery datasets (raw and analytics)
- Service accounts
- IAM permissions

### 5. Start Airflow

```bash
cd orchestration/airflow
docker-compose up -d --build
```

### 6. Create Airflow Admin User

```bash
docker-compose run airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 7. Access Airflow UI

Open your browser and navigate to:
```
http://localhost:8080
```

Login with:
- **Username:** `admin`
- **Password:** `admin`

### 8. Trigger the DAG

In the Airflow UI:
1. Find the `dbt_bigquery_pipeline` DAG
2. Toggle it to "On"
3. Click the play button to trigger manually

---

## 🔄 Pipeline Workflow

The Airflow DAG orchestrates the following tasks:

1. **`dbt_deps`** - Install dbt package dependencies
2. **`dbt_run`** - Execute dbt models to transform raw data
3. **`dbt_test`** - Run data quality tests
4. **`load_csv_to_bigquery`** - Load raw CSV files into BigQuery

### Task Dependencies

```
dbt_deps → dbt_run → dbt_test
         ↘ load_csv_to_bigquery
```

---

## 🧪 Data Generation

Generate synthetic marketplace data using Python and Faker:

```bash
cd data_generator_script_and_files
python generate_data.py
```

This creates CSV files with:
- User data
- Product catalog
- Click events
- Order transactions
- Reviews

---

## 🧠 dbt Models

### Staging Layer (`models/staging/`)
Clean and standardize raw data:
- `stg_clicks.sql`
- `stg_orders.sql`
- `stg_products.sql`
- `stg_users.sql`

### Analytics Layer (`models/analytics/`)
Business logic and metrics:
- `fct_orders.sql` - Order facts
- `dim_products.sql` - Product dimensions
- `dim_users.sql` - User dimensions

### Run dbt Locally

```bash
cd marketplace_dbt
dbt run
dbt test
dbt docs generate
dbt docs serve
```

---

## 🐳 Docker Services

The `docker-compose.yml` file defines:

| Service | Purpose | Port |
|---------|---------|------|
| **postgres** | Airflow metadata database | 5432 |
| **airflow-webserver** | Airflow UI | 8080 |
| **airflow-scheduler** | Task scheduling and execution | - |

### Useful Docker Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Rebuild containers
docker-compose up -d --build

# Execute commands inside container
docker-compose exec airflow-webserver bash
```

---

## 🧱 Infrastructure as Code

Terraform manages:
- **BigQuery datasets** (raw, analytics)
- **Service accounts** with appropriate permissions
- **IAM roles** for secure access

### Terraform Commands

```bash
cd infrastructure/terraform

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply changes
terraform apply

# Destroy infrastructure
terraform destroy
```

---

## 🎓 What You'll Learn

Building this project teaches you:

✅ **Data Generation** - Create realistic synthetic datasets with Faker  
✅ **Cloud Data Warehousing** - Work with Google BigQuery  
✅ **Data Transformation** - Build dbt models with SQL  
✅ **Workflow Orchestration** - Schedule and monitor with Airflow  
✅ **Containerization** - Deploy with Docker and Docker Compose  
✅ **Infrastructure as Code** - Automate cloud resources with Terraform  
✅ **Best Practices** - Version control, testing, documentation  

---

## 📊 Monitoring and Logs

### Airflow Logs
- **Location:** `orchestration/airflow/logs/`
- **View in UI:** Click on any task in the Airflow UI to see logs

### dbt Logs
- **Location:** `marketplace_dbt/logs/dbt.log`
- **Run logs:** Generated after each `dbt run`

### BigQuery
Monitor queries and costs in the [GCP Console](https://console.cloud.google.com/bigquery)

---

## 🔒 Security Best Practices

- ✅ **Never commit credentials** - Use `.gitignore` to exclude keys
- ✅ **Use service accounts** - Follow principle of least privilege
- ✅ **Rotate keys regularly** - Update service account keys periodically
- ✅ **Store secrets securely** - Keep keys in `~/.keys/` locally
- ✅ **Use environment variables** - For sensitive configuration

---

## 🐛 Troubleshooting

### Common Issues

**1. "No such file or directory" for service account key**
- Ensure the key is at `~/.keys/service-account-key.json`
- Check docker-compose volume mount
- Verify profiles.yml keyfile path

**2. Airflow tasks failing**
- Check task logs in Airflow UI
- Verify BigQuery permissions
- Ensure dbt models are valid SQL

**3. dbt connection errors**
- Validate profiles.yml configuration
- Test service account permissions in GCP Console
- Check BigQuery dataset exists

**4. Docker containers not starting**
- Run `docker-compose logs` to see errors
- Ensure ports 8080 and 5432 are available
- Try `docker-compose down && docker-compose up --build`

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is open source and available for educational purposes.

---

## 📧 Contact

**Bianca Niemann**  
GitHub: [@BiancaNiemann](https://github.com/BiancaNiemann)

---

## 🙏 Acknowledgments

- **Faker** - For synthetic data generation
- **dbt Labs** - For the amazing dbt framework
- **Apache Airflow** - For workflow orchestration
- **Google Cloud Platform** - For BigQuery and cloud infrastructure
- **Terraform** - For infrastructure as code capabilities

---

⭐ **Star this repo** if you find it helpful!

