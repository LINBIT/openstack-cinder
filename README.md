# Configuring Cinder Volume Services for LINBIT SDS on RHOSO

This document describes how to configure the OpenStack Block Storage service (Cinder)
to use LINBIT SDS (LINSTOR) as a storage backend on Red Hat OpenStack Services on
OpenShift (RHOSO 18.0).

## Overview

LINBIT SDS exposes block storage to Cinder through a volume driver
(`cinder.volume.drivers.linstordrv.LinstorDriver`) that communicates with the LINSTOR
controller over HTTP(S). Volumes are placed on LINSTOR satellites and exported to Nova
compute nodes via iSCSI.

See the [LINBIT SDS User Guide – OpenStack/Cinder chapter](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openstack-linstor)
for a full description of the driver and its configuration options.

## Prerequisites

Before configuring Cinder you must have:

1. **LINBIT SDS installed** in the `linbit-sds` namespace and a healthy
   `LinstorCluster` with at least one storage pool defined (see the
   [LINBIT SDS User Guide - OpenShift chapter](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openshift).
2. **LINSTOR controller reachable** from the `openstack` namespace. The in-cluster
   service name is `linstor-controller.linbit-sds.svc`. If TLS is enabled on the
   LINSTOR API (recommended), you will also need client certificates.
3. **MetalLB address pool** for the storage network already configured, because
   iSCSI requires a stable IP on the storage network that compute nodes can reach.

The general RHOSO prerequisites (NMState, MetalLB, cert-manager, RHOSO operator) are
described in the
[Red Hat OpenStack Services on OpenShift – Planning your deployment](https://docs.redhat.com/en/documentation/red_hat_openstack_services_on_openshift/18.0/html/planning_your_deployment/assembly_infrastructure-and-system-requirements)
guide.

---

## Using the LINBIT Cinder Image

LINBIT provides a pre-built Cinder volume image that includes the LINSTOR driver and
the `python-linstor` client library, available on the Red Hat Ecosystem Catalog:

```
registry.connect.redhat.com/linbit/openstack-cinder-volume-rhel9
```

Register this image for each named Cinder volume backend using an `OpenStackVersion` CR.
The keys under `cinderVolumeImages` must exactly match the keys used in the
`cinderVolumes` section of `OpenStackControlPlane` (see below):

```yaml
apiVersion: core.openstack.org/v1beta1
kind: OpenStackVersion
metadata:
  name: openstack-control-plane
spec:
  customContainerImages:
    cinderVolumeImages:
      linstor-iscsi-1: registry.connect.redhat.com/linbit/openstack-cinder-volume-rhel9:18.0
      linstor-iscsi-2: registry.connect.redhat.com/linbit/openstack-cinder-volume-rhel9:18.0
```

See the RHOSO documentation on
[Configuring the Block Storage service](https://docs.redhat.com/en/documentation/red_hat_openstack_services_on_openshift/18.0/html/configuring_the_block_storage_service)
for general guidance on custom Cinder backend images.

---

## Mounting the Node Hostname

The LINSTOR driver needs to know which LINSTOR satellite node the `cinder-volume` pod
is running on. It reads this from a file configured via `linstor_hostname_file`.
The simplest approach is to mount the host's `/etc/hostname` into the pod.

Add this to the top-level `extraMounts` section of `OpenStackControlPlane` and list
every backend name under `propagation`:

```yaml
spec:
  extraMounts:
  - extraVol:
    - mounts:
      - mountPath: /etc/linstor/hostname
        name: etc-hostname
        readOnly: true
      volumes:
      - name: etc-hostname
        hostPath:
          path: /etc/hostname
          type: File
      propagation:
      - linstor-iscsi-1
      - linstor-iscsi-2
```

This is required because the driver uses the hostname to identify itself within the
LINSTOR cluster when placing or accessing resources.

---

## Creating LoadBalancer Services for iSCSI

Each `cinder-volume` pod runs an iSCSI target (LIO). Compute nodes connect to the
target IP directly, so each backend instance must have a **stable IP address** on the
storage network. Use a MetalLB `LoadBalancer` service to provide this.

Create one `Service` per backend instance. MetalLB assigns a fixed IP from the storage
address pool:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cinder-backend-linstor-1
  namespace: openstack
  annotations:
    metallb.universe.tf/address-pool: storage
    metallb.universe.tf/loadBalancerIPs: 172.18.0.80   # adjust to your storage network
spec:
  type: LoadBalancer
  selector:
    backend: linstor-iscsi-1       # must match the cinderVolumes key
    component: cinder-volume
    service: cinder
  ports:
  - port: 3260
    name: iscsi
    appProtocol: TCP
    targetPort: 3260
---
apiVersion: v1
kind: Service
metadata:
  name: cinder-backend-linstor-2
  namespace: openstack
  annotations:
    metallb.universe.tf/address-pool: storage
    metallb.universe.tf/loadBalancerIPs: 172.18.0.81
spec:
  type: LoadBalancer
  selector:
    backend: linstor-iscsi-2
    component: cinder-volume
    service: cinder
  ports:
  - port: 3260
    name: iscsi
    appProtocol: TCP
    targetPort: 3260
```

> The label `backend: <cinderVolumes-key>` is automatically applied by the RHOSO
> operator to each `cinder-volume` pod.

---

## Configuring the Cinder Volume Backends

All Cinder backends are configured inside `spec.cinder.template.cinderVolumes` in the
`OpenStackControlPlane` CR. Each entry becomes a separate `cinder-volume` deployment.

### Driver configuration options

The following options are used in the LINSTOR backend configuration. See the
[LINBIT SDS User Guide](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openstack-linstor)
for the full option reference.

| Option | Description |
|---|---|
| `volume_driver` | Always `cinder.volume.drivers.linstordrv.LinstorDriver` |
| `volume_backend_name` | Arbitrary identifier shown in `openstack volume backend pool list` |
| `target_helper` | iSCSI target helper; use `lioadm` for LIO (the default on RHEL) |
| `target_protocol` | Set to `iscsi` |
| `target_secondary_ip_addresses` | The LoadBalancer IP advertised to iSCSI initiators |
| `linstor_uris` | LINSTOR controller URI(s). Use the in-cluster service: `http://linstor-controller.linbit-sds.svc:3370` (HTTP) or `https://…:3371` (HTTPS) |
| `linstor_hostname_file` | Path to a file containing the satellite hostname (`/etc/linstor/hostname` when using the `extraMounts` approach above) |
| `linstor_timeout` | Seconds to wait for a LINSTOR API response (default `60`; raise to `300` for large clusters) |
| `linstor_force_udev` | Set `False` inside containers (udev is not running inside the pod) |
| `linstor_direct` | Set `False` for iSCSI transport |
| `linstor_default_storage_pool_name` | Storage pool to use when no volume type overrides it |
| `linstor_default_resource_group_name` | Resource group to use when no volume type is provided (default `DfltRscGrp`) |
| `linstor_client_key` | Path to PEM private key for HTTPS connections to LINSTOR |
| `linstor_client_cert` | Path to PEM client certificate for HTTPS connections to LINSTOR |
| `linstor_trusted_ca` | Path to PEM CA certificate to verify the LINSTOR controller |

### cinderVolumes configuration

Pass the LoadBalancer IP from the Service above as `target_secondary_ip_addresses` so
that the iSCSI target advertises the correct address to initiators:

```yaml
spec:
  cinder:
    template:
      cinderVolumes:
        linstor-iscsi-1:
          networkAttachments:
          - storage
          replicas: 1
          customServiceConfig: |
            [linstor-iscsi-1]
            target_helper=lioadm
            target_protocol=iscsi
            target_secondary_ip_addresses=172.18.0.80   # LoadBalancer IP from above
            volume_backend_name=linstor-iscsi-1
            volume_driver=cinder.volume.drivers.linstordrv.LinstorDriver
            linstor_force_udev=False
            linstor_direct=False
            linstor_uris=http://linstor-controller.linbit-sds.svc:3370
            linstor_hostname_file=/etc/linstor/hostname
            linstor_timeout=300
        linstor-iscsi-2:
          networkAttachments:
          - storage
          replicas: 1
          customServiceConfig: |
            [linstor-iscsi-2]
            target_helper=lioadm
            target_protocol=iscsi
            target_secondary_ip_addresses=172.18.0.81
            volume_backend_name=linstor-iscsi-2
            volume_driver=cinder.volume.drivers.linstordrv.LinstorDriver
            linstor_force_udev=False
            linstor_direct=False
            linstor_uris=http://linstor-controller.linbit-sds.svc:3370
            linstor_hostname_file=/etc/linstor/hostname
            linstor_timeout=300
```

The INI section name (e.g. `[linstor-iscsi-1]`) must match the `volume_backend_name`
value and the key in `cinderVolumes`.

---

## TLS Configuration (optional but recommended)

If the LINSTOR API is protected with TLS (configured via `LinstorCluster.spec.apiTLS`),
mount the client certificate and CA into the `cinder-volume` pods and reference them
in the driver configuration.

1. Copy the relevant secret from the `linbit-sds` namespace into `openstack`:

   ```bash
   oc get secret linstor-api-ca -n linbit-sds -o yaml \
     | sed 's/namespace: linbit-sds/namespace: openstack/' \
     | oc apply -f -
   ```

2. Add an `extraMounts` entry to propagate the certificates to the affected backends.

3. Update `linstor_uris` and add the TLS options to each backend's `customServiceConfig`:

   ```ini
   linstor_uris=https://linstor-controller.linbit-sds.svc:3371
   linstor_client_key=/etc/linstor/tls/tls.key
   linstor_client_cert=/etc/linstor/tls/tls.crt
   linstor_trusted_ca=/etc/linstor/tls/ca.crt
   ```

---

## Applying the Configuration

Apply the manifests to the `openstack` namespace:

```bash
oc apply -f openstack_control_plane.yaml
```

Wait for the Cinder pods to become ready:

```bash
oc get pods -n openstack -l service=cinder
```

---

## Verification

List the registered Cinder backend pools:

```bash
openstack volume backend pool list
```

You should see one entry per configured backend (e.g.
`cinder-volume-linstor-iscsi-1@linstor-iscsi-1#linstor-iscsi-1`).

Create a test volume to confirm end-to-end functionality:

```bash
openstack volume create --size 1 --type <your-volume-type> test-linstor
openstack volume show test-linstor
```

---

## References

- [Red Hat OpenStack Services on OpenShift 18.0 – Configuring the Block Storage service](https://docs.redhat.com/en/documentation/red_hat_openstack_services_on_openshift/18.0/html/configuring_the_block_storage_service)
- [Red Hat OpenStack Services on OpenShift 18.0 – Planning your deployment](https://docs.redhat.com/en/documentation/red_hat_openstack_services_on_openshift/18.0/html/planning_your_deployment)
- [LINBIT SDS User Guide – OpenShift chapter](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openshift)
- [LINBIT SDS User Guide – OpenStack/Cinder chapter](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openstack-linstor)
