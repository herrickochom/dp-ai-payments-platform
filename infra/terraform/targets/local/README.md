# Local Terraform Target

Classification: reference integration environment.

This target is implemented first.

It exists to validate the portable platform architecture and Terraform
deployment model on the developer workstation / WSL environment.

It is not a production environment.

The local target may use local/container infrastructure to represent
production semantics, including Kafka, MinIO, PostgreSQL, Nessie, Trino,
Airflow and related platform services.

The local target must not claim:

- production high availability
- production disaster recovery
- cloud IAM
- cloud KMS
- multi-zone resilience
- production remote Terraform state

CDC remains fail-closed unless a source is explicitly governed and activated.

The local Terraform implementation must not rematerialise the beneficiary
token-link, reset Kafka offsets, delete Raw objects, run recovery restoration,
or perform uncontrolled dbt execution.

## Infrastructure ownership

Docker Compose remains the authoritative local application topology.

Terraform is the local deployment governance control plane. It must not
duplicate ownership of individual Compose containers, networks or volumes.

The local ownership model is:

    Terraform
        |
        +-- validates deployment prerequisites
        +-- validates deployment contracts
        +-- governs approved deployment invocation
        |
        +--> Docker Compose
                 |
                 +-- services
                 +-- containers
                 +-- networks
                 +-- volumes
                 +-- profiles

Terraform Docker provider resources are intentionally prohibited for this
target because they would create dual lifecycle ownership with Compose.

This restriction applies to the local reference target only. Cloud targets
may use provider-specific Terraform resources as their infrastructure
authority.
