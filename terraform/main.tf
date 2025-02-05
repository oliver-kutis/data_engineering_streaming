terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.14.1"
    }
  }
}

provider "google" {
  credentials = file(var.gcp_credentials)
  project     = var.gcp.project
  region      = var.gcp.region
}


# resource "google_bigquery_dataset" "rt_dataset" {
#   dataset_id    = var.gcp.bigquery.realtime.dataset_id
#   friendly_name = var.gcp.bigquery.realtime.friendly_name
#   description   = var.gcp.bigquery.realtime.description
#   project       = var.gcp.project
# }

# resource "google_storage_bucket" "rt_bucket" {
#   name          = var.gcp.storage.realtime.bucket_name
#   location      = var.gcp.region
#   storage_class = "STANDARD"
#   force_destroy = false

#   lifecycle_rule {
#     condition {
#       age = var.gcp.storage.realtime.max_age
#     }

#     action {
#       type = "Delete"
#     }
#   }
# }


resource "google_storage_bucket" "buckets" {
  for_each      = var.gcp.storage_buckets
  name          = each.value.bucket_name
  location      = var.gcp.region
  storage_class = "STANDARD"
  force_destroy = false

  lifecycle_rule {
    condition {
      age = each.value.max_age
    }

    action {
      type = "Delete"
    }
  }
}

# output "bigquery_dataset_id" {
#   value = google_bigquery_dataset.dataset.dataset_id
# }
# output "gcp_buckets" {
#   value = {
#     for key, value in google_storage_bucket.buckets : key => {
#       name    = value.name
#       max_age = value.lifecycle_rule.condition.age
#     }
#   }
# }
