terraform {
  backend "gcs" {
    prefix = "terraform/environments/dev"
  }
}
