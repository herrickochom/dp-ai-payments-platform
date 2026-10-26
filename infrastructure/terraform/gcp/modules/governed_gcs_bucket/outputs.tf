output "bucket_name" {
  description = "Name of the governed bucket."
  value       = google_storage_bucket.governed.name
}

output "bucket_url" {
  description = "GCS URI for the governed bucket."
  value       = google_storage_bucket.governed.url
}

output "storage_class" {
  description = "Configured initial storage class."
  value       = google_storage_bucket.governed.storage_class
}

output "location" {
  description = "Configured bucket location."
  value       = google_storage_bucket.governed.location
}
