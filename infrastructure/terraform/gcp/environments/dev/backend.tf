terraform {
  backend "gcs" {
    bucket = "pdm-dev-tfstate-91932345778"
    prefix = "terraform/environments/dev"
  }
}
