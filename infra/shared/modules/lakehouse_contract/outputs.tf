output "cloud" {
  description = "Cloud this contract describes (aws or azure)."
  value       = var.cloud
}

output "scheme" {
  description = "Hadoop filesystem scheme for the layer URIs (s3a or abfss)."
  value       = local.scheme
}

output "layer_uris" {
  description = "Map of bronze/silver/gold to root URI. Feed these into config/lakehouse.<env>.yaml."
  value       = var.layer_uris

  # Terraform 1.5 variable validations cannot read other variables, so the cloud/scheme
  # cross-check lives here.
  precondition {
    condition     = alltrue([for u in values(var.layer_uris) : startswith(u, "${local.scheme}://")])
    error_message = "layer URI scheme does not match cloud (aws => s3a://, azure => abfss://)."
  }
}

output "principals" {
  description = "Map of loader/transformer/analyst to {id, principal_id}."
  value       = local.principals
}
