# The nightly maintenance workflow: EventBridge Scheduler -> Step Functions -> Lambda tasks.

locals {
  account = "000000000000"
  image   = "${local.account}.dkr.ecr.us-east-1.amazonaws.com/${aws_ecr_repository.app.name}:latest"
}

resource "aws_ecr_repository" "app" {
  name = "lossproof"
}

data "aws_iam_policy_document" "assume" {
  for_each = toset(["lambda", "states", "scheduler"])

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["${each.key}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  for_each           = data.aws_iam_policy_document.assume
  name               = "lossproof-${each.key}"
  assume_role_policy = each.value.json
}

# One function for every task; the workflow says which task in the payload.
# No IAM policies are attached: the emulator does not enforce them, and this project makes no IAM claims.
resource "aws_lambda_function" "tasks" {
  function_name = "lossproof-tasks"
  role          = aws_iam_role.service["lambda"].arn
  package_type  = "Image"
  image_uri     = local.image
  timeout       = 600
  memory_size   = 1024

  image_config {
    entry_point = ["python", "-m", "awslambdaric"]
    command     = ["lossproof.tasks.handler"]
  }

  environment {
    variables = {
      AWS_ENDPOINT_URL  = "http://floci:4566"
      LOSSPROOF_RUN     = "main"
      LOSSPROOF_PG_HOST = aws_db_instance.source.address
      LOSSPROOF_PG_PORT = aws_db_instance.source.port
    }
  }
}

resource "aws_sfn_state_machine" "nightly" {
  name     = "lossproof-nightly"
  role_arn = aws_iam_role.service["states"].arn

  definition = jsonencode({
    Comment = "Compact the lake, then prove it still matches the source."
    StartAt = "Maintain"
    States = {
      Maintain = {
        Type       = "Task"
        Resource   = aws_lambda_function.tasks.arn
        Parameters = { task = "maintain" }
        ResultPath = "$.maintain"
        Retry      = [{ ErrorEquals = ["States.TaskFailed"], IntervalSeconds = 5, MaxAttempts = 2 }]
        Next       = "Reconcile"
      }
      Reconcile = {
        Type       = "Task"
        Resource   = aws_lambda_function.tasks.arn
        Parameters = { task = "reconcile" }
        ResultPath = "$.reconcile"
        Next       = "Matches"
      }
      Matches = {
        Type    = "Choice"
        Choices = [{ Variable = "$.reconcile.wrong_rows", NumericGreaterThan = 0, Next = "Diverged" }]
        Default = "Done"
      }
      Diverged = {
        Type  = "Fail"
        Error = "LakeDiverged"
        Cause = "The lake differs from the source. See reports/reconcile/latest.json in the lake bucket."
      }
      Done = { Type = "Succeed" }
    }
  })
}

resource "aws_scheduler_schedule" "nightly" {
  name                = "lossproof-nightly"
  schedule_expression = "cron(0 2 * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.nightly.arn
    role_arn = aws_iam_role.service["scheduler"].arn
  }
}

output "state_machine" {
  value = aws_sfn_state_machine.nightly.arn
}
