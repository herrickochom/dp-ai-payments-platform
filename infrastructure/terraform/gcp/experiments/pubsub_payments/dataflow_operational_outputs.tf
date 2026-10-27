output "dataflow_operational_bucket_name" {
  description = "Dedicated Dataflow operational bucket."
  value       = google_storage_bucket.dataflow_operational.name
}

output "dataflow_staging_location" {
  description = "Dataflow staging location."
  value       = "gs://${google_storage_bucket.dataflow_operational.name}/staging/"
}

output "dataflow_temp_location" {
  description = "Dataflow temporary files location."
  value       = "gs://${google_storage_bucket.dataflow_operational.name}/temp/"
}
