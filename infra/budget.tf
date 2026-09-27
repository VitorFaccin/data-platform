# Cost control declared in code: on a self-funded project, runaway spend must
# alert before the invoice does.
#
# NOTE: a budget ALERTS, it does not STOP spending — GCP has no universal hard cap.
# The real cost strategy is architectural: run, screenshot, `terraform destroy`.
#
# With no explicit notification channel, GCP e-mails the Billing Account
# Administrators — on a personal account, that is you. Good enough here.

resource "google_billing_budget" "monthly" {
  # billingbudgets is the one API never enabled by default — wait for apis.tf.
  depends_on = [google_project_service.required]

  billing_account = var.billing_account_id
  display_name    = "data-platform monthly"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "BRL"
      units         = tostring(var.budget_amount_brl)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }
}
