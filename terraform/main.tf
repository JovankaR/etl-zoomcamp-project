terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.23.0"
    }
    random = {
      source  = "hashicorp/random"
    }
  }
}

# -----------------------
# Provider
# -----------------------

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

# -----------------------
# Random suffix (for unique bucket name)
# -----------------------

resource "random_id" "suffix" {
  byte_length = 4
}

# -----------------------
# Storage Bucket (Data Lake)
# -----------------------

resource "google_storage_bucket" "data_lake" {
  name     = "${var.data_lake_bucket_name}-${random_id.suffix.hex}"
  location = var.location

  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    environment = "etl-zoomcamp"
    purpose     = "data-lake"
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }
}

# -----------------------
# BigQuery Dataset (Warehouse)
# -----------------------

resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.bq_dataset_id
  location   = var.location

  labels = {
    environment = "etl-zoomcamp"
    purpose     = "warehouse"
  }

  # Optional: explicit access for ETL SA
  access {
    role          = "OWNER"
    user_by_email = google_service_account.etl_sa.email
  }
}

# -----------------------
# Service Account for ETL
# -----------------------

resource "google_service_account" "etl_sa" {
  account_id   = var.etl_service_account_id
  display_name = var.etl_service_account_display_name
  description  = "Service account for ETL pipeline"
}

# -----------------------
# IAM - Privilege Roles
# -----------------------

resource "google_project_iam_member" "etl_storage_admin" {
  project = var.gcp_project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

resource "google_project_iam_member" "etl_bigquery_admin" {
  project = var.gcp_project
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

resource "google_project_iam_member" "etl_dataproc" {
  count   = var.enable_dataproc ? 1 : 0
  project = var.gcp_project
  role    = "roles/dataproc.admin"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

resource "google_project_iam_member" "etl_dataproc_worker" {
  project = var.gcp_project
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

resource "google_service_account_iam_member" "etl_sa_self_user" {
  service_account_id = google_service_account.etl_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.etl_sa.email}"
}

# -----------------------
# Outputs
# -----------------------

output "data_lake_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "Name of the GCS data lake bucket"
}

output "bq_dataset_id" {
  value       = google_bigquery_dataset.warehouse.dataset_id
  description = "BigQuery dataset ID"
}

output "etl_service_account_email" {
  value       = google_service_account.etl_sa.email
  description = "Service account email for ETL"
}