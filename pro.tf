# pro.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"  # Versión del proveedor AWS
    }
  }

  required_version = ">= 1.0.0"  # Versión mínima de Terraform
}

provider "aws" {
  region = var.region  # Ahora usa la variable 'region' en lugar de un valor fijo
}

