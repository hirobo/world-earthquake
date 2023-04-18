variable "project" {
  description = "Your GCP Project ID"
}

variable "data_lake_bucket" {
  description = "This label will be used as a part of data lake bucket name"
  default     = "world_earthquake_dl"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "EU"
  type        = string
}

variable "vm_region" {
  description = "Region for VM"
  default     = "europe-west3"
  type        = string
}

variable "vm_zone" {
  description = "Zone for VM"
  default     = "europe-west3-a"
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

