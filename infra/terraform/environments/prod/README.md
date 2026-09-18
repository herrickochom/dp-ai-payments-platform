# prod Environment

This directory is reserved for the prod Terraform composition.

Current status:

- provider: not selected
- backend: not configured
- resources: not defined
- deployment: disabled
- automatic CDC activation: prohibited

Provider-specific infrastructure must not be introduced until the deployment
target is explicitly approved.

The environment must consume governed reusable modules rather than duplicating
infrastructure definitions.
