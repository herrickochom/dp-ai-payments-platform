provider "google" {
  project = var.project_id
  region  = var.region
}

module "pubsub_payments" {
  source = "../../modules/pubsub_payments"

  project_id        = var.project_id
  topic_name        = "pdm-payments-poc-v1"
  subscription_name = "pdm-payments-poc-sub-v1"
  schema_name       = "pdm-payment-event-v1"

  avro_definition             = file("${path.module}/payment-event-v1.avsc")
  allowed_persistence_regions = ["europe-west2"]
}
