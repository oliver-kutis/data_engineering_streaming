variable "gcp_credentials" {
  description = "GCP credentials"
  type        = string
  default     = "../.keys/gcp-service-account.json"
}
variable "gcp" {
  description = "GCP configuration"
  type = object({
    project  = string
    location = string
    region   = string
    bigquery_datasets = object({
      realtime = object({
        dataset_id    = string
        friendly_name = string
        description   = string
      })
      batch = object({
        dataset_id    = string
        friendly_name = string
        description   = string
      })
    })
    storage_buckets = object({
      realtime = object({
        bucket_name = string
        max_age     = string
      })
      batch = object({
        bucket_name = string
        max_age     = string
      })
    })
  })
  default = {
    project  = "playtest-learn"
    location = "EU"
    region   = "europe-west1"
    bigquery_datasets = {
      realtime = {
        dataset_id    = "rt_eventsim"
        friendly_name = "Real-time Events Simulation Dataset"
        description   = "Storage of real-time events simulation data from 'eventsim' application"
      }
      batch = {
        dataset_id    = "batch_eventsim"
        friendly_name = "Batch Events Simulation Dataset"
        description   = "Storage of real-time events simulation data from 'eventsim' application"
      }
    }
    storage_buckets = {
      realtime = {
        # bucket_name = "rt-eventsim"
        bucket_name = "rt-eventsim"
        max_age     = "1"
      }
      batch = {
        bucket_name = "batch-eventsim"
        max_age     = "30"
      }
    }
  }
}
