output "lake" {
  value = google_dataplex_lake.payments.name
}
output "zones" {
  value = [google_dataplex_zone.raw.name]
}
output "assets" {
  value = [google_dataplex_asset.gcs_raw_v1.name]
}
