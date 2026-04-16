variable "resource_group_name" {
  default = "rg-devops-prometheus-lab"
}

variable "location" {
  default = "swedencentral"
}

variable "vm_name" {
  default = "vm-docker-host-prometheus"
}

variable "admin_username" {
  default = "azureuser"
}

variable "repo_url" {
  default = "https://github.com/olena-novosad/open-data-ai-analytics-prometheus.git"
}