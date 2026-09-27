# Inputs. Real values live in terraform.tfvars (gitignored) — copy the .example.

variable "project_id" {
  description = "GCP project that hosts every resource in this file set."
  type        = string
}

variable "region" {
  description = "Default region. southamerica-east1 = São Paulo."
  type        = string
  default     = "southamerica-east1"
}

variable "billing_account_id" {
  description = "Billing account for the budget alert (format XXXXXX-XXXXXX-XXXXXX)."
  type        = string
}

variable "budget_amount_brl" {
  description = "Monthly budget in BRL that triggers the alert e-mails."
  type        = number
  default     = 20
}
