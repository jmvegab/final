# sensorCloud.tf

# Crear una VPC
resource "aws_vpc" "main_vpc" {
  cidr_block           = var.vpc_cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "main_vpc"
  }
}

# Subred pública en us-east-1a
resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.main_vpc.id
  cidr_block              = var.public_subnet_cidr
  map_public_ip_on_launch = true
  availability_zone       = "us-east-1a"  # Definir la AZ
  tags = {
    Name = "public_subnet"
  }
}

# Subred privada en us-east-1a
resource "aws_subnet" "private_subnet" {
  vpc_id            = aws_vpc.main_vpc.id
  cidr_block        = var.private_subnet_cidr
  availability_zone = "us-east-1a"  # AZ 1
  tags = {
    Name = "private_subnet"
  }
}

# Subred privada adicional en us-east-1b
resource "aws_subnet" "private_subnet_2" {
  vpc_id            = aws_vpc.main_vpc.id
  cidr_block        = "10.0.3.0/24"  # CIDR único para la segunda subred
  availability_zone = "us-east-1b"   # AZ 2
  tags = {
    Name = "private_subnet_2"
  }
}

# Internet Gateway para la subred pública
resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.main_vpc.id
  tags = {
    Name = "main_internet_gateway"
  }
}

# Tabla de rutas para la subred pública
resource "aws_route_table" "public_route_table" {
  vpc_id = aws_vpc.main_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.internet_gateway.id
  }
}

# Asociar la tabla de rutas a la subred pública
resource "aws_route_table_association" "public_route_assoc" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.public_route_table.id
}

# Grupo de seguridad para EC2 (SSH, HTTP)
resource "aws_security_group" "ec2_sg" {
  vpc_id = aws_vpc.main_vpc.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Permitir SSH desde cualquier IP
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Permitir tráfico HTTP
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ec2_sg"
  }
}

# Grupo de seguridad para RDS (Permitir acceso solo desde EC2)
resource "aws_security_group" "rds_sg" {
  vpc_id = aws_vpc.main_vpc.id

  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    security_groups = [aws_security_group.ec2_sg.id]  # Permitir acceso solo desde EC2
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "rds_sg"
  }
}

resource "tls_private_key" "example" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "deployer" {
  key_name   = "deployer-key"
  public_key = tls_private_key.example.public_key_openssh
}

resource "local_file" "private_key" {
  content  = tls_private_key.example.private_key_pem
  filename = "${path.module}/deployer-key.pem"
}

# Instancia EC2 con Docker
resource "aws_instance" "docker_instance" {
  ami           = var.ami_id
  instance_type = var.instance_type
  subnet_id     = aws_subnet.public_subnet.id
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  key_name      = var.key_pair_name

  depends_on = [
    aws_security_group.ec2_sg,
    aws_security_group.rds_sg
  ]

  user_data = <<-EOF
              #!/bin/bash
              # Actualizar los paquetes
              sudo dnf update -y

              # Instalar Docker
              sudo dnf install -y docker

              # Iniciar Docker
              sudo systemctl start docker

              # Habilitar Docker para que se inicie en cada reinicio
              sudo systemctl enable docker

              # Agregar el usuario ec2-user al grupo docker para que pueda usar Docker sin sudo
              sudo usermod -a -G docker ec2-user

              # Descargar la imagen de Docker desde Docker Hub
              sudo docker pull jmvegab09/prueba-my-python-app:latest
            EOF

  tags = {
    Name = "DockerInstance"
  }
}


# Crear la base de datos MySQL en RDS
resource "aws_db_instance" "mydb" {
  allocated_storage    = 20  # 20 GB, dentro del nivel gratuito
  engine               = "mysql"
  engine_version       = "8.0.34"  # Cambiar a una versión compatible
  instance_class       = "db.t3.micro"  # Instancia gratuita
  db_name              = "mydatabase"
  username             = var.db_username
  password             = var.db_password
  skip_final_snapshot  = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name = aws_db_subnet_group.mydb_subnet.id
}

# Grupo de subredes para la base de datos
resource "aws_db_subnet_group" "mydb_subnet" {
  name       = "mydb_subnet_group"
  subnet_ids = [aws_subnet.private_subnet.id, aws_subnet.private_subnet_2.id]  # Añadir ambas subredes privadas
  tags = {
    Name = "mydb_subnet_group"
  }
}
