output "public_ip_address" {
  description = "The public IP address of the virtual machine"
  value       = azurerm_public_ip.pip.ip_address
}

output "web_url" {
  description = "URL to access the web application"
  value       = "http://${azurerm_public_ip.pip.ip_address}:80"
}

output "grafana_url" {
  description = "URL to access Grafana"
  value       = "http://${azurerm_public_ip.pip.ip_address}:3000"
}

output "prometheus_url" {
  description = "URL to access Prometheus"
  value       = "http://${azurerm_public_ip.pip.ip_address}:9090"
}