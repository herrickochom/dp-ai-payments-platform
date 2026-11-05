variable "project_id" {
  type        = string
  description = "GCP project hosting the Dataplex lake."
}
variable "region" {
  type        = string
  description = "Region for lake, zones, assets (must match BQ datasets)."
}
variable "bucket" {
  type        = string
  description = "GCS bucket registered as RAW asset. Asset covers the whole bucket."
}
variable "curated_datasets" {
  type        = list(string)
  description = "BigQuery datasets to register as CURATED assets (must already exist)."
  default     = []
}
variable "enable_discovery" {
  type        = bool
  description = "Toggles Dataplex discovery on zones/assets. Billed; requires approval."
  default     = false
}
variable "enable_scans" {
  type        = bool
  description = "Toggles premium-SKU data quality scans. NO free tier. Off by default."
  default     = false
}
