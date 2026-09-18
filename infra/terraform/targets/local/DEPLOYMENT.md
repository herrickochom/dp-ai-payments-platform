# Local Terraform Deployment Boundary

Terraform governs the local deployment contract.

Docker Compose remains the authoritative owner of local services, containers,
networks, volumes and profiles.

## Local deployment sequence

The intended controlled sequence is:

1. validate host prerequisites
2. validate Terraform configuration
3. validate Docker Compose configuration
4. inspect the proposed deployment action
5. require explicit operator intent
6. invoke the approved Compose deployment boundary
7. run post-deployment acceptance checks

Terraform must not create Docker containers, Docker networks or Docker volumes
for this target.

Terraform plan must remain side-effect free.

Destructive cleanup is not part of the normal deployment path.

The local target is a reference integration environment and must not be
described as a production deployment.
