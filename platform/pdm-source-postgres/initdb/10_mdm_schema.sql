CREATE SCHEMA mdm;
REVOKE ALL ON SCHEMA mdm FROM PUBLIC;

CREATE TABLE mdm.beneficiary_master_sources (
  source_record_id text PRIMARY KEY, master_domain text NOT NULL CHECK (master_domain = 'beneficiary'),
  source_system text NOT NULL, source_entity text NOT NULL, beneficiary_id text NOT NULL,
  nin text, nin_verified boolean NOT NULL, name text NOT NULL, date_of_birth date,
  phone text, alternative_phone text, email text, household_id text,
  region text, district text, county text, sub_county text, parish text, village text,
  is_active boolean NOT NULL, source_created_at timestamptz, source_updated_at timestamptz
);
COMMENT ON TABLE mdm.beneficiary_master_sources IS 'RESTRICTED_IDENTITY';

CREATE TABLE mdm.sacco_master_sources (
  source_record_id text PRIMARY KEY, master_domain text NOT NULL CHECK (master_domain = 'sacco'),
  source_system text NOT NULL, source_entity text NOT NULL, sacco_id text NOT NULL,
  name text NOT NULL, registration_number text, wendi_account text,
  region text, district text, county text, sub_county text, parish text, village text,
  is_active boolean NOT NULL, source_created_at timestamptz, source_updated_at timestamptz
);
COMMENT ON TABLE mdm.sacco_master_sources IS 'CONTROLLED_MASTER_DATA';

CREATE TABLE mdm.agent_master_sources (
  source_record_id text PRIMARY KEY, master_domain text NOT NULL CHECK (master_domain = 'agent'),
  source_system text NOT NULL, source_entity text NOT NULL, agent_id text NOT NULL,
  agent_code text, name text NOT NULL, phone text, registration_number text,
  network_provider text, region text, district text, county text, sub_county text,
  parish text, village text, is_active boolean NOT NULL, verified boolean NOT NULL,
  source_created_at timestamptz, source_updated_at timestamptz
);
COMMENT ON TABLE mdm.agent_master_sources IS 'CONTROLLED_MASTER_DATA';

CREATE TABLE mdm.geography_master_sources (
  source_record_id text PRIMARY KEY, master_domain text NOT NULL CHECK (master_domain = 'geography'),
  source_system text NOT NULL, source_entity text NOT NULL, location_key text NOT NULL,
  region text, district text, county text, sub_county text, parish text, village text
);
COMMENT ON TABLE mdm.geography_master_sources IS 'REFERENCE_DATA';

CREATE TABLE mdm.golden_beneficiaries_restricted (
  mdm_beneficiary_id text PRIMARY KEY, source_system text NOT NULL, source_record_id text NOT NULL,
  nin text, nin_verified boolean NOT NULL, name text NOT NULL, date_of_birth date,
  phone text, alternative_phone text, email text, household_id text,
  region text, district text, county text, sub_county text, parish text, village text,
  is_active boolean NOT NULL, source_created_at timestamptz, source_updated_at timestamptz,
  record_classification text NOT NULL CHECK (record_classification = 'RESTRICTED_IDENTITY')
);
COMMENT ON TABLE mdm.golden_beneficiaries_restricted IS 'RESTRICTED_IDENTITY';

CREATE TABLE mdm.golden_saccos (LIKE mdm.sacco_master_sources INCLUDING ALL);
ALTER TABLE mdm.golden_saccos ADD COLUMN mdm_sacco_id text NOT NULL;
ALTER TABLE mdm.golden_saccos ADD CONSTRAINT golden_saccos_id_unique UNIQUE (mdm_sacco_id);
COMMENT ON TABLE mdm.golden_saccos IS 'CONTROLLED_MASTER_DATA';

CREATE TABLE mdm.golden_agents (LIKE mdm.agent_master_sources INCLUDING ALL);
ALTER TABLE mdm.golden_agents ADD COLUMN mdm_agent_id text NOT NULL;
ALTER TABLE mdm.golden_agents ADD CONSTRAINT golden_agents_id_unique UNIQUE (mdm_agent_id);
COMMENT ON TABLE mdm.golden_agents IS 'CONTROLLED_MASTER_DATA';

CREATE TABLE mdm.golden_geography (LIKE mdm.geography_master_sources INCLUDING ALL);
ALTER TABLE mdm.golden_geography ADD COLUMN mdm_location_id text NOT NULL;
ALTER TABLE mdm.golden_geography ADD CONSTRAINT golden_geography_id_unique UNIQUE (mdm_location_id);
COMMENT ON TABLE mdm.golden_geography IS 'REFERENCE_DATA';

CREATE TABLE mdm.source_crosswalk (
  master_domain text NOT NULL, golden_record_id text,
  source_system text NOT NULL, source_entity text NOT NULL, source_record_id text NOT NULL,
  match_rule text NOT NULL, match_status text NOT NULL
    CHECK (match_status IN ('MATCHED', 'AMBIGUOUS', 'QUARANTINED', 'DEACTIVATED')),
  valid_from timestamptz NOT NULL, valid_to timestamptz, is_current boolean NOT NULL,
  PRIMARY KEY (master_domain, source_system, source_record_id)
);
COMMENT ON TABLE mdm.source_crosswalk IS 'RESTRICTED_LINKAGE';

CREATE TABLE mdm.beneficiary_identity_alerts_restricted (
  master_domain text NOT NULL CHECK (master_domain = 'beneficiary'),
  golden_record_id text NOT NULL, source_system text NOT NULL,
  source_entity text NOT NULL, source_record_id text NOT NULL,
  alert_type text NOT NULL, severity text NOT NULL,
  PRIMARY KEY (source_system, source_entity, source_record_id, alert_type)
);
COMMENT ON TABLE mdm.beneficiary_identity_alerts_restricted IS 'RESTRICTED_IDENTITY';

-- Deliberately narrow: no beneficiary or restricted-linkage table is published.
CREATE PUBLICATION pdm_mdm_ordinary_pub FOR TABLE mdm.sacco_master_sources
  WITH (publish = 'insert,update,delete');
