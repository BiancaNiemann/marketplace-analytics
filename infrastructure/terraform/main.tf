terraform {

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = var.raw_dataset_name
  location   = var.location
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = var.analytics_dataset_name
  location   = var.location
}

resource "google_service_account" "dbt" {
  account_id   = "dbt-runner"
  display_name = "dbt Runner Service Account"
}

resource "google_project_iam_member" "dbt_bigquery_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigquery.dataViewer"
  ])

  project = var.project
  role    = each.key
  member  = "serviceAccount:${google_service_account.dbt.email}"
}


