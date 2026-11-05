output "lake" {
  value = google_dataplex_lake.payments.name
}
output "zones" {
  value = [google_dataplex_zone.raw.name, google_dataplex_zone.curated.name]
}
output "assets" {
  value = concat(
    [google_dataplex_asset.gcs_raw_v2.name],
    [for k, v in google_dataplex_asset.bq : v.name]
  )
}
