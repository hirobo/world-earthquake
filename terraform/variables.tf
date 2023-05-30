variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "EU"
  type        = string
}

variable "location" {
  description = "Location"
  default     = "europe-west3"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "bq_dataset_raw" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "world_earthquake_raw"
}

variable "gcp_key_file" {}

