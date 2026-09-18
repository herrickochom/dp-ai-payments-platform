# PDM Infrastructure as Code

Terraform is the production Infrastructure-as-Code control plane.

Planned scope includes:

- networking
- service identities and IAM
- storage
- Kafka / CDC infrastructure
- database infrastructure
- secret/KMS references
- Airflow infrastructure
- observability
- archive/retention infrastructure
- disaster recovery infrastructure

CI requirements:

1. terraform fmt -check
2. terraform validate
3. security/static analysis
4. terraform plan
5. plan artifact retention
6. approval for protected environments

CD requirements:

1. approved immutable artefact
2. controlled terraform apply
3. application deployment
4. database/CDC migration controls
5. post-deployment validation
6. rollback/recovery procedure

Production secrets must not be committed to Terraform source.
Sensitive values must use an approved secret/KMS backend.
