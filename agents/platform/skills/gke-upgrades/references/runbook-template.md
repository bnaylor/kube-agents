# Runbook Command Templates

Standard command sequences for GKE upgrades. Replace placeholders: `CLUSTER_NAME`, `ZONE`, `TARGET_VERSION`, `NODE_POOL_NAME`.

> A change goes out as a pull request to the GitOps repository, never as a write from here: Kubernetes objects as YAML through `gke-manifest-generation`, cluster settings and other Google Cloud resources as Config Connector YAML through `gcp-config-connector`, opened with `submit-suggestion`. Each profile's config offers only the `gke` MCP server's read tools, and the broker refuses `kubectl` and `gcloud` writes.

Each change step below is a pull request editing the cluster's `ContainerCluster` or the pool's `ContainerNodePool`; each check step is a read command.

## Table of Contents
- [Pre-flight](#pre-flight) (Line 16-35)
- [Control plane upgrade](#control-plane-upgrade) (Line 36-47)
- [Node pool upgrade (Standard only)](#node-pool-upgrade-standard-only) (Line 48-60)
- [Maintenance window configuration](#maintenance-window-configuration) (Line 61-68)
- [Rollback/Downgrade guidance](#rollbackdowngrade-guidance) (Line 69-117)

## Pre-flight

```bash
# Current versions
gcloud container clusters describe CLUSTER_NAME \
  --zone ZONE \
  --format="table(name, currentMasterVersion, nodePools[].version)"

# Available versions for channel
gcloud container get-server-config --zone ZONE \
  --format="yaml(channels)"

# Deprecated API usage
kubectl get --raw /metrics | grep apiserver_request_total | grep deprecated

# Cluster health
kubectl get nodes
kubectl get pods -A | grep -v Running | grep -v Completed
```

## Control plane upgrade

Propose `spec.minMasterVersion: TARGET_VERSION` on the cluster's `ContainerCluster` as a pull request. After it merges and applies, verify (wait ~10-15 min):

```bash
gcloud container clusters describe CLUSTER_NAME \
  --zone ZONE \
  --format="value(currentMasterVersion)"

kubectl get pods -n kube-system
```

## Node pool upgrade (Standard only)

Propose the pool's surge settings (`spec.upgradeSettings.maxSurge: MAX_SURGE`, `spec.upgradeSettings.maxUnavailable: MAX_UNAVAILABLE`) and its version (`spec.version: TARGET_VERSION`) on its `ContainerNodePool` as a pull request. After it merges and applies:

```bash
# Monitor progress
watch 'kubectl get nodes -o wide -L cloud.google.com/gke-nodepool'

# Verify
gcloud container node-pools list --cluster CLUSTER_NAME --zone ZONE
kubectl get pods -A | grep -v Running | grep -v Completed
```

## Maintenance window configuration

Each of these is proposed as a pull request on the cluster's `ContainerCluster` through `gcp-config-connector`:

- **Recurring maintenance window**: `spec.maintenancePolicy.recurringWindow` with `startTime` and `endTime` (`YYYY-MM-DDTHH:MM:SSZ`) and `recurrence` (e.g. `FREQ=WEEKLY;BYDAY=SA`).
- **Maintenance exclusion (up to 90 days)**: an entry in `spec.maintenancePolicy.maintenanceExclusion` with `exclusionName`, `startTime`, `endTime` and `exclusionOptions.scope`.
- **Persistent exclusion (until End of Support)** and **node pool level exclusions**: each has no declarative path in a Config Connector repository (the KCC v1.158.0 schema has no field for it), so the agent blocks rather than inventing a field.

## Rollback/Downgrade guidance

- **Control Plane Patches**: Can be downgraded to the target older patch version.
- **Control Plane Minors**: Rollback is only available during the first step of the 2-step upgrade process.
- **Node Pools (Minor & Patch)**: Can be downgraded directly by targeting the older version, OR by creating a new pool at the old version and migrating workloads (safer).

### Downgrade Control Plane (Patch or Step-1 Minor)

A downgrade to `TARGET_PREVIOUS_VERSION` has no declarative path in a Config Connector repository (`spec.minMasterVersion` is a floor GKE upgrades past, not a downgrade), so the agent blocks rather than inventing a field.

### Downgrade Node Pool (Direct)

Propose `spec.version: TARGET_PREVIOUS_VERSION` on the pool's `ContainerNodePool` as a pull request.

### Downgrade Node Pool (Safe migration - recommended)

Propose a new replacement `ContainerNodePool` named `NODE_POOL_NAME-rollback` at the previous version (`spec.version: PREVIOUS_VERSION`, `spec.initialNodeCount: NUM_NODES`, `spec.nodeConfig.machineType: MACHINE_TYPE`) as a pull request. Workloads move off the old pool when a later pull request removes its `ContainerNodePool`. Before that:

```bash
# Record current PDB state BEFORE touching anything — this file is how you put
# it back. Do not skip; a relaxed PDB left in place is an outage waiting for
# the next voluntary disruption.
kubectl get pdb -A -o yaml > /tmp/pdb-backup-$(date +%s).yaml
kubectl get pdb -A   # ALLOWED DISRUPTIONS = 0 will block the drain
```

If a PDB blocks the drain, relax it **temporarily** and restore it as a required
step of the same runbook — never as a follow-up someone may forget:

1. Relax the blocking PDB, only after confirming the replacement pool is Ready and
   can accept the workload: a pull request that removes `minAvailable` from its
   manifest and sets `maxUnavailable: "100%"`.
2. After the drain, verify workloads are Running on the replacement pool:

   ```bash
   kubectl get pods -o wide --field-selector spec.nodeName!='' -A | grep NODE_POOL_NAME-rollback
   ```

3. RESTORE the original PDB — mandatory, not optional: a pull request that puts back
   the `minAvailable` or `maxUnavailable` value the backup records for `PDB_NAME` and
   clears the field step 1 set (an integer as-is, a percentage quoted, e.g. `"50%"`).
4. Confirm the restore took effect:

   ```bash
   kubectl get pdb -A   # ALLOWED DISRUPTIONS should match the pre-drain values
   ```

> A rollback runbook that relaxes PDBs without restoring them has silently
> removed the cluster's disruption protection. Always include steps 3 and 4.
