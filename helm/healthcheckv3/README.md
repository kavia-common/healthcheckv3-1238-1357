# healthcheckv3 Helm Chart

This chart deploys the healthcheckv3 Monolithic Healthcheck Script as a single-instance Kubernetes application designed for periodic invocation (e.g., by an external scheduler like Airflow). The application does not serve incoming requests; it performs a healthcheck and communicates externally (kubectl, Kafka, Loki).

Key features:
- Single-instance Deployment (replicaCount=1)
- Environment selection (dev|stage|prod) via values
- YAML-based configuration mounted through ConfigMap
- Secure secret injection via Kubernetes Secrets
- Optional Service (disabled by default)
- Optional Secret volume mount and PVC for logs
- Pod security contexts and resource requests/limits

## Prerequisites

- Kubernetes 1.23+
- Container image for the healthcheck application
- kubectl accessible within the container image
- Kafka and Loki endpoints reachable from the cluster if enabled in config
- Metrics Server installed on the target cluster (for `kubectl top`)

## Install

```sh
# Basic install (dev environment by default)
helm upgrade --install hc ./healthcheckv3 \
  --namespace du-health --create-namespace \
  --set image.repository=ghcr.io/your-org/healthcheckv3 \
  --set image.tag=1.0.0 \
  --set app.siteId=site-001 \
  --set app.clusterId=cluster-a
```

## Choosing the environment

Set the environment to one of dev, stage, prod:

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set app.environment=prod \
  --set app.siteId=site-001 \
  --set app.clusterId=cluster-a
```

This will mount a ConfigMap file named `<env>.yaml` into the container at `/app/healthcheckv3MonolithicHealthcheckScript/config/<env>.yaml`, aligned with the script’s expectation.

## Overriding YAML configuration

You can override the YAML for each environment by setting `.Values.config.dev`, `.Values.config.stage`, or `.Values.config.prod`.

Example (override a Kafka broker in stage):

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set app.environment=stage \
  --set-string "config.stage=$(cat <<'EOF'
environment: stage
logging:
  level: INFO
  file: /tmp/du_healthcheck_stage.log
kubernetes:
  site_label_key: siteId
pods:
  pod1:
    label_selector: "app=pod1"
    required_containers: ["du-core", "du-aux"]
  pod2:
    label_selector: "app=pod2"
    required_containers: ["du-rt", "du-mon"]
ignore_containers:
  pod1: []
  pod2: []
thresholds:
  cpu_max: 1200
  memory_max: 1536
  disk_max: 80
  rach_degraded: 500
  rach_degrading_min: 500
  rach_healthy_min: 1000
kafka:
  brokers: ["stage-broker:9092"]
  topic: "du-health"
  tls_enabled: false
  ca_path:
  username:
  password:
loki:
  endpoint: "http://stage-loki:3100/loki/api/v1/push"
  tenant_id:
log_paths:
  sctp: "/var/log/du/sctp.log"
  rach: "/var/log/du/rach.log"
  pucch: "/var/log/du/pucch.log"
EOF
)"
```

Alternatively, provide your own file content via a values file.

## Passing secrets securely

Enable secret creation and provide keys in `.Values.secrets.data`. The chart maps each secret key as an environment variable of the same name. This is suitable for:
- Kubeconfig retrieval for the Vault stub: KUBECONFIG_<cluster_id>
- Kafka SASL username/password (if used)
- Any other sensitive parameters

Example using `--set-file` for kubeconfig content:

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set secrets.create=true \
  --set-string app.siteId=site-001 \
  --set-string app.clusterId=cluster-a \
  --set-file secrets.data.KUBECONFIG_cluster-a=/path/to/kubeconfig
```

If you need to mount secrets as files (e.g., CA bundle), enable secret volume mounting:

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set secrets.create=true \
  --set secretMount.enabled=true \
  --set secretMount.mountPath=/opt/healthcheck/secret \
  --set-file secrets.data.ssl-ca.pem=/path/to/ca-bundle.crt
```

The secret volume will be mounted read-only at the specified path. Note that environment variables are still set for each secret key.

## Optional Service

The application does not serve requests. If needed (for sidecar discovery, etc.), you can enable a Service:

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set service.enabled=true \
  --set service.port=8080
```

## Pod security and resources

Defaults enforce:
- Non-root user and group
- No privilege escalation
- Read-only root filesystem
- CPU and memory requests/limits

Adjust via `values.yaml` if required.

## Probes

Since the app is not network-serving, probes are simple exec checks:
- Liveness: `python -V`
- Readiness: verifies required env vars are present

You can modify or disable via `.Values.probes`.

## Persistence

By default, logs can go to stdout or container filesystem. To persist logs:
- Enable persistence and use an existing PVC, or keep `emptyDir` (non-persistent)

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set persistence.enabled=true \
  --set persistence.existingClaim=my-log-pvc \
  --set persistence.mountPath=/var/log
```

## Environment variables

- APP_ENV: selected via `app.environment`
- HC_SITE_ID: set via `app.siteId`
- HC_CLUSTER_ID: set via `app.clusterId`
- Extra envs: `app.extraEnv`
- Any key in `secrets.data` becomes an environment variable

## Image

Provide your own image repository and tag:

```sh
helm upgrade --install hc ./healthcheckv3 \
  --set image.repository=ghcr.io/your-org/healthcheckv3 \
  --set image.tag=1.0.0
```

## Example minimal production install

```sh
helm upgrade --install hc ./healthcheckv3 \
  --namespace du-health --create-namespace \
  --set app.environment=prod \
  --set app.siteId=site-001 \
  --set app.clusterId=cluster-a \
  --set image.repository=ghcr.io/your-org/healthcheckv3 \
  --set image.tag=1.0.0 \
  --set secrets.create=true \
  --set-file secrets.data.KUBECONFIG_cluster-a=/secure/kubeconfigs/cluster-a
```

## Notes

- The script fetches kubeconfig content from environment variable `KUBECONFIG_<cluster_id>` via the Vault stub. Inject that via secrets as shown above.
- ConfigMap delivers the YAML file at path expected by the script (`./config/<env>.yaml` under the repo path `/app/healthcheckv3MonolithicHealthcheckScript/config` in the container).
- No Service is required by default.

## Uninstall

```sh
helm uninstall hc -n du-health
```

## Troubleshooting

- Ensure Metrics Server is installed for `kubectl top` to work.
- Verify that your container image includes `kubectl`.
- Confirm Kafka and Loki endpoints are reachable from the cluster network.
- Check logs:
  - `kubectl logs deploy/<release>-healthcheckv3 -n du-health`

