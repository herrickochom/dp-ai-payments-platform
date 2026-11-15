output "instance_connection_name" {
  value = google_sql_database_instance.mdm.connection_name
}
output "private_ip_address" {
  value     = google_sql_database_instance.mdm.private_ip_address
  sensitive = true
}
output "database_name" {
  value = google_sql_database.mdm.name
}
