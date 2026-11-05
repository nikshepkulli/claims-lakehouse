variable "cloud" {
  description = "Which cloud the wrapped storage module targets."
  type        = string

  validation {
    condition     = contains(["aws", "azure"], var.cloud)
    error_message = "cloud must be one of: aws, azure."
  }
}

variable "layer_uris" {
  description = "Map of medallion layer to root URI, as emitted by a cloud storage module (s3a:// on AWS, abfss:// on Azure)."
  type        = map(string)

  validation {
    condition     = toset(keys(var.layer_uris)) == toset(["bronze", "silver", "gold"])
    error_message = "layer_uris must have exactly the keys bronze, silver, gold."
  }

  validation {
    condition = alltrue([
      for u in values(var.layer_uris) :
      can(regex("^(s3a://[a-z0-9.-]+|abfss://[a-z0-9-]+@[a-z0-9]+\\.dfs\\.core\\.windows\\.net)/?", u))
    ])
    error_message = "every layer URI must start with s3a://<bucket> or abfss://<filesystem>@<account>.dfs.core.windows.net."
  }
}

variable "principals" {
  description = "Map of persona (loader, transformer, analyst) to an object with at least id and principal_id (IAM role ARN on AWS; managed identity resource ID and object ID on Azure). Extra attributes are dropped."
  type        = map(any)

  validation {
    condition     = toset(keys(var.principals)) == toset(["loader", "transformer", "analyst"])
    error_message = "principals must have exactly the keys loader, transformer, analyst."
  }

  validation {
    condition = alltrue([
      for p in values(var.principals) : can(p.id) && can(p.principal_id)
    ])
    error_message = "every principal needs id and principal_id attributes."
  }
}
