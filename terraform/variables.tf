# -----------------------
# GCP Project and Region
# -----------------------

variable "gcp_project" {
  type        = string
  description = "ID of the GCP project where resources will be created"
}

variable "gcp_region" {
  type        = string
  description = "GCP region for resources"
  default     = "europe-west6"
}

# -----------------------
# Storage Bucket (Data Lake)
# -----------------------

variable "data_lake_bucket_name" {
  type        = string
  description = "Name of the GCS bucket for the data lake"
  default = "etl-zoomcamp"
}

variable "location" {
  type        = string
  description = "Geographical location for bucket and dataset (e.g., EU, US, or a region like europe-west6)"
  default     = "EU"
}

# -----------------------
# BigQuery Dataset (Warehouse)
# -----------------------

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset ID for the data warehouse"
  default     = "mental_health_dw"
}

# -----------------------
# ETL Service Account
# -----------------------

variable "etl_service_account_id" {
  type        = string
  description = "Service account ID for ETL pipeline"
  default     = "etl-service-account"
}

variable "etl_service_account_display_name" {
  type        = string
  description = "Display name for ETL service account"
  default     = "ETL Pipeline Service Account"
}

# -----------------------
# Optional: Dataproc
# -----------------------

variable "enable_dataproc" {
  type        = bool
  description = "Whether to assign Dataproc permissions to the ETL service account"
  default     = true
}