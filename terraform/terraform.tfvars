# terraform.tfvars

# This file contains the variable values for Terraform. 
# You can edit these values to match your GCP project and resources.
# only gcp_project is required to be changed, the rest can be left as is.
# Terraform will make sure bucket names are unique by appending a random suffix

gcp_project                    = "de-zoomcamp-project-2026"
gcp_region                     = "europe-west6"          
data_lake_bucket_name          = "de-zoomcamp-data-lake" 
location                       = "EU"          