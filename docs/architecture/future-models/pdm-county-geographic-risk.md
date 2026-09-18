# Disabled models — not part of the active dbt project.
#
## geography_future/
#
# cns_pdm_county_geographic_risk.sql
#   County is a legal administrative level between district and sub-county,
#   but no platform source currently carries a county attribute (checked
#   staging, silver and gld_dim_pdm_geography). Without a county column the
#   model cannot resolve its grain. Re-enable only after a county attribute
#   lands in slv_pdm_saccos, slv_pdm_beneficiaries or the geography
#   dimension.
#
## retired/
#
# cns_pdm_geographic_risk.sql
#   Superseded by cns_pdm_parish_geographic_risk.sql (same parish grain).
#   Kept for lineage reference; do not re-enable.