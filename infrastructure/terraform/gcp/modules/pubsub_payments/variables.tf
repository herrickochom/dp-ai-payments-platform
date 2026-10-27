variable "project_id" {
  type = string
}

variable "topic_name" {
  type = string
}

variable "subscription_name" {
  type = string
}

variable "schema_name" {
  type = string
}

variable "avro_definition" {
  type = string
}


variable "allowed_persistence_regions" {
  description = "GCP regions permitted to store Pub/Sub messages."
  type        = list(string)
}
