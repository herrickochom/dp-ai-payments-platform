-- MDM schema — auto-generated — idempotent

CREATE SCHEMA IF NOT EXISTS mdm;

CREATE TABLE IF NOT EXISTS mdm.beneficiary_master_sources (
    "alternative_phone" text,
    "beneficiary_id" text,
    "county" text,
    "date_of_birth" text,
    "district" text,
    "email" text,
    "household_id" text,
    "is_active" boolean,
    "master_domain" text,
    "name" text,
    "nin" text,
    "nin_verified" boolean,
    "parish" text,
    "phone" text,
    "region" text,
    "source_created_at" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.sacco_master_sources (
    "county" text,
    "district" text,
    "is_active" boolean,
    "master_domain" text,
    "name" text,
    "parish" text,
    "region" text,
    "registration_number" text,
    "sacco_id" text,
    "source_created_at" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "village" text,
    "wendi_account" text
);

CREATE TABLE IF NOT EXISTS mdm.agent_master_sources (
    "agent_code" text,
    "agent_id" text,
    "county" text,
    "district" text,
    "is_active" boolean,
    "master_domain" text,
    "name" text,
    "network_provider" text,
    "parish" text,
    "phone" text,
    "region" text,
    "registration_number" text,
    "source_created_at" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "verified" boolean,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.geography_master_sources (
    "county" text,
    "district" text,
    "location_key" text,
    "master_domain" text,
    "parish" text,
    "region" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "sub_county" text,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.golden_beneficiaries_restricted (
    "alternative_phone" text,
    "county" text,
    "date_of_birth" text,
    "district" text,
    "email" text,
    "household_id" text,
    "is_active" boolean,
    "mdm_beneficiary_id" text,
    "name" text,
    "nin" text,
    "nin_verified" boolean,
    "parish" text,
    "phone" text,
    "record_classification" text,
    "region" text,
    "source_created_at" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.golden_saccos (
    "county" text,
    "district" text,
    "is_active" boolean,
    "master_domain" text,
    "mdm_sacco_id" text,
    "name" text,
    "parish" text,
    "region" text,
    "registration_number" text,
    "sacco_id" text,
    "source_created_at" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "village" text,
    "wendi_account" text
);

CREATE TABLE IF NOT EXISTS mdm.golden_agents (
    "agent_code" text,
    "agent_id" text,
    "county" text,
    "district" text,
    "is_active" boolean,
    "master_domain" text,
    "mdm_agent_id" text,
    "name" text,
    "network_provider" text,
    "parish" text,
    "phone" text,
    "region" text,
    "registration_number" text,
    "source_created_at" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "source_updated_at" text,
    "sub_county" text,
    "verified" boolean,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.golden_geography (
    "county" text,
    "district" text,
    "location_key" text,
    "master_domain" text,
    "mdm_location_id" text,
    "parish" text,
    "region" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "sub_county" text,
    "village" text
);

CREATE TABLE IF NOT EXISTS mdm.source_crosswalk (
    "golden_record_id" text,
    "is_current" boolean,
    "master_domain" text,
    "match_rule" text,
    "match_status" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text,
    "valid_from" text,
    "valid_to" text
);

CREATE TABLE IF NOT EXISTS mdm.beneficiary_identity_alerts_restricted (
    "alert_type" text,
    "golden_record_id" text,
    "master_domain" text,
    "severity" text,
    "source_entity" text,
    "source_record_id" text,
    "source_system" text
);

-- Grants
GRANT CONNECT ON DATABASE pdm_mdm TO pdm_mdm_writer;
GRANT USAGE ON SCHEMA mdm TO pdm_mdm_writer;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA mdm TO pdm_mdm_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA mdm TO pdm_mdm_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA mdm GRANT SELECT, INSERT, DELETE ON TABLES TO pdm_mdm_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA mdm GRANT USAGE, SELECT ON SEQUENCES TO pdm_mdm_writer;
