output "governed_storage_domains" {
  description = "Logical storage domain definitions."

  value = {
    for name, domain in local.storage_domains :
    name => {
      storage_class = domain.storage_class
      purpose       = domain.purpose
      datasets      = domain.datasets
    }
  }
}

output "protected_storage_categories" {
  description = "Categories requiring separate storage architecture."
  value       = local.protected_storage_categories
}

output "governed_storage_deployment_enabled" {
  description = "Whether governed storage deployment is enabled."
  value       = var.enable_governed_storage_deployment
}

output "governed_storage_bucket_names" {
  description = "Names of instantiated governed storage buckets."
  value = {
    for name, bucket in module.governed_storage_domains :
    name => bucket.bucket_name
  }
}
