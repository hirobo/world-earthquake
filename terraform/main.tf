terraform {
  required_version = ">= 1.0"
  backend "gcs" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file("${var.gcp_key_file}")
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "world_earthquake_pipeline_dl_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location = var.region

  # Optional, but recommended settings:
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

/*   lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90 // days
    }
  }
 */
  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "earthquake_raw_dataset" {
  dataset_id  = "earthquake_raw"
  project     = var.project
  location    = var.region
  description = "dataset for raw data"
}

# Artifact Registry
resource "google_artifact_registry_repository" "artifact_registry_repository" {
  provider      = google
  location      = var.location
  repository_id = "world-earthquake-pipeline"
  format        = "DOCKER"
}
