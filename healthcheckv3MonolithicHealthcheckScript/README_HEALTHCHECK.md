# DU Healthcheck Script

Standalone monolithic Python script to assess DU site health in Kubernetes.

How to run:
- Ensure kubectl is installed and accessible.
- Ensure kubeconfig is retrievable via Vault stub: set environment variable `KUBECONFIG_<cluster_id>` to kubeconfig content.
- Select configuration environment (dev/stage/prod) via third argument or APP_ENV env var.
- Execute:
  python du_healthcheck.py <site_id> <cluster_id> [env]

Outputs:
- Prints JSON health report to stdout.
- Sends the report to Kafka topic configured in YAML; on failures, error events are pushed to Loki.

Configuration:
- Files under `config/`:
  - dev.yaml, stage.yaml, prod.yaml
- Key sections:
  - logging: level, file
  - kubernetes: site_label_key
  - pods: label selectors and required containers
  - thresholds: cpu_max (m), memory_max (Mi), disk_max (%), rach thresholds
  - kafka: brokers, topic, tls settings
  - loki: endpoint, tenant_id
  - log_paths: sctp, rach, pucch

Security Notes:
- Do not log secrets; kubeconfig is handled in memory and transient temp file path for kubectl.
- Replace VaultClient with actual secure integration in production.

Limitations:
- Requires metrics-server for `kubectl top` to function.
- Container names and pod label selectors must be set correctly in YAML.
