output "catalog_name" {
  value = try(google_biglake_iceberg_catalog.payments[0].name, null)
}
output "table_name" {
  value = try(google_biglake_iceberg_table.payment_events[0].name, null)
}
output "catalog_enabled" {
  value = var.enable_biglake_iceberg_catalog
}
