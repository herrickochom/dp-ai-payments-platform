output "pilot_catalog_name" {
  value = try(google_biglake_iceberg_catalog.pilot[0].name, null)
}
output "pilot_table_name" {
  value = try(google_biglake_iceberg_table.pilot[0].name, null)
}
output "pilot_enabled" {
  value = var.enable_kp014_iceberg_pilot
}
