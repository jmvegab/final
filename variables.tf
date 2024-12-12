# variables.tf

variable "vpc_cidr_block" {
  description = "CIDR Block para la VPC"
  type        = string
}

variable "public_subnet_cidr" {
  description = "CIDR Block para la subred pública"
  type        = string
}

variable "private_subnet_cidr" {
  description = "CIDR Block para la subred privada"
  type        = string
}

variable "ami_id" {
  description = "AMI ID de la instancia EC2"
  type        = string
}

variable "instance_type" {
  description = "Tipo de instancia EC2"
  type        = string
}

variable "key_pair_name" {
  description = "Nombre del Key Pair para SSH"
  type        = string
}

variable "db_username" {
  description = "Nombre de usuario para la base de datos"
  type        = string
}

variable "db_password" {
  description = "Contraseña de la base de datos"
  type        = string
}

variable "region" {
  description = "Región donde se desplegará la infraestructura"
  type        = string
}

