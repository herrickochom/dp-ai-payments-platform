# Stage 10 planning policy only. No resources are created by this file.
# These variables must be wired into future Dataflow launch configuration;
# Terraform validation alone does not enforce runtime worker count or teardown.
variable "stage10_pilot_max_dataflow_workers" {
  type    = number
  default = 1
  validation {
    condition     = var.stage10_pilot_max_dataflow_workers == 1
    error_message = "Stage 10 pilot is restricted to one Dataflow worker."
  }
}
variable "stage10_pilot_max_runtime_minutes" {
  type    = number
  default = 30
  validation {
    condition     = var.stage10_pilot_max_runtime_minutes >= 1 && var.stage10_pilot_max_runtime_minutes <= 30
    error_message = "Stage 10 pilot runtime must be 1–30 minutes."
  }
}
variable "stage10_pilot_authorised" {
  type    = bool
  default = false
}
