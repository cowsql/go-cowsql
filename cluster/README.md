# go-cowsql cluster

The `cluster` package exposes an opinionated pattern for setting up a resilient COWSQL cluster
following the design in use by [Incus](https://github.com/lxc/incus) in production clusters since 2018.

## Implementation and Usage

The main entry point to the `cluster` package is `cluster.Gateway`.
```go
// Initialize the gateway.
gateway, _ := gateway.NewGateway(ctx, node, clusterCert, serverCertFunc, state)

// Setup your API here, including gateway.HandlerFuncs.
go myServer.Serve(myListener)

// Open the COWSQL database.
driver, _ := gateway.Driver()
sql.Register("my-cowsql", driver)
db, _ := sql.Open("my-cowsql", driver)
gateway.SetClusterDB(db)
```

`NewGateway` depends on several interfaces to be set up by the consuming project:

* `State`:
  Related to managing certificates on the filesystem, as well as listener setup and authentication.
* `Node`:
  Used for per-member local store of cluster members.
* `Cluster`:
  Content of the global COWSQL database set up by the `cluster` package.
  These methods are used to provide implementations for cluster membership, role changes, recovery, and heartbeats.
* `CertInfo`:
  TLS certificate management for shared cluster certificates and per-member server certificates for TLS-based intra-cluster communication.

## Example

An example package is provided, showing basic cluster setup with a recurring heartbeat and role rebalancing.

Note that while TLS is set up in this example, it is not verified for ease of testing.

```bash
# Start 3 daemons:
cluster daemon --address 10.0.0.101:8001 --name c1 --dir /tmp/cowsql-c1
cluster daemon --address 10.0.0.101:8002 --name c2 --dir /tmp/cowsql-c2
cluster daemon --address 10.0.0.101:8003 --name c3 --dir /tmp/cowsql-c3

# Bootstrap a cluster with 1 member:
cluster client bootstrap --address 10.0.0.101:8001

# Join an existing cluster from an uninitialized daemon:
cluster client join --address 10.0.0.101:8002 --target 10.0.0.101:8001
cluster client join --address 10.0.0.101:8003 --target 10.0.0.101:8001

# List cluster members:
cluster client list --address 10.0.0.101:8002

Name: "c1"  Address: "10.0.0.101:8001"  Role: "voter"  Offline: false
Name: "c2"  Address: "10.0.0.101:8002"  Role: "voter"  Offline: false
Name: "c3"  Address: "10.0.0.101:8003"  Role: "voter"  Offline: false
```
