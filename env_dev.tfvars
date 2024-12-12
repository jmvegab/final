# env_dev.tfvars

vpc_cidr_block       = "10.0.0.0/16"
public_subnet_cidr   = "10.0.1.0/24"
private_subnet_cidr  = "10.0.2.0/24"
ami_id               = "ami-06b21ccaeff8cd686"  # Cambia este valor si usas otra región
instance_type        = "t2.micro"
key_pair_name        = "deployer-key"  # Key Pair para acceder por SSH
db_username          = "root"
db_password          = "password123"
region               = "us-east-1"  # Cambia esta región según tus necesidades

