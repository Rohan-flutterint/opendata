# RFC 0003: Catalog

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a catalog system for OpenData that serves as a central management plane for OpenData storage systems. The catalog provides a single point to manage metadata about the systems ("slates") a user has installed, including their names, types, and object storage configuration. The catalog itself is implemented as a slate backed by SlateDB, following the same patterns as other OpenData subsystems.

## Motivation

OpenData comprises multiple storage subsystems (log, timeseries, vector, etc.) that share a common foundation on SlateDB. As users deploy multiple instances of these systems, several cross-cutting operational concerns emerge:

1. **Discovery** — There is no unified way to enumerate which slates exist within an environment. Operators must track this information externally or inspect object storage directly.

2. **Configuration management** — Each slate requires object storage configuration (bucket, path prefix, credentials). Without a central registry, this configuration is scattered and difficult to audit.

3. **Lifecycle management** — Creating and deleting slates involves coordinating metadata across multiple locations. A catalog provides a single point of control for these operations.

4. **Operational tooling** — Cross-cutting tools (backup, monitoring, migration) need a way to discover and enumerate slates without subsystem-specific knowledge.

A catalog addresses these concerns by providing:

- A single source of truth for slate metadata
- Consistent registration and deletion workflows
- A foundation for future operational tooling that works across subsystem types

The catalog is itself a slate backed by SlateDB, ensuring it benefits from the same durability and operational characteristics as the systems it manages.

### Hypothetical Workflow

```
$ opendata slate list
NAME        TYPE        BUCKET
events      log         s3://acme-data/events
metrics     timeseries  s3://acme-data/metrics

$ opendata slate register --name orders --type log --bucket s3://acme-data/orders
Registered slate 'orders'

$ opendata slate list
NAME        TYPE        BUCKET
events      log         s3://acme-data/events
metrics     timeseries  s3://acme-data/metrics
orders      log         s3://acme-data/orders

$ opendata slate describe orders
Name:   orders
Type:   log
Bucket: s3://acme-data/orders

$ opendata slate delete orders
Deleted slate 'orders'

$ opendata slate config events
storage:
  type: SlateDb
  path: events
  object_store:
    type: Aws
    region: us-west-2
    bucket: acme-data
```

## Goals

- Define "slate" as the fundamental unit of an OpenData system
- Specify the metadata required to describe a slate (name, type, object storage location)
- Establish the catalog as a SlateDB-backed registry of slate metadata
- Define the registration process for adding new slates to the catalog
- Define the deletion process for removing slates from the catalog
- Ensure the catalog can manage slates of all types (log, timeseries, vector)

## Non-Goals

_To be completed in a future revision._

## Design

### Communication Model

The catalog does not operate as a persistent service with a communication endpoint. Instead, all interaction with the catalog occurs through its SlateDB-backed storage. This follows directly from SlateDB's Reader/Writer model, where components temporarily assume one of two roles:

- **Writer** — A component that opens the catalog with write access to mutate state (register slates, update metadata, record status changes).
- **Reader** — A component that opens a read-only view of the catalog to observe state changes.

This model enables coordination between loosely-coupled components without requiring a running service or explicit RPC. Components communicate implicitly by writing state that other components read.

#### Example: Registration and Provisioning

Consider a flow where a user registers a new slate and a provisioning system responds:

1. A CLI tool temporarily assumes the **Writer** role to register a new slate in the catalog.
2. A provisioning system (e.g., a Kubernetes operator) running as a **Reader** observes the new registration.
3. The provisioning system creates the corresponding infrastructure (pods, services, etc.).
4. The provisioning system temporarily assumes the **Writer** role to update the slate's status, indicating provisioning is complete.
5. The CLI (or other tooling) as a **Reader** can observe the updated status.

```
┌─────────┐         ┌─────────────────┐         ┌─────────────────┐
│   CLI   │         │     Catalog     │         │   Provisioner   │
│         │         │    (SlateDB)    │         │   (K8s, etc.)   │
└────┬────┘         └────────┬────────┘         └────────┬────────┘
     │                       │                           │
     │  [Writer] register    │                           │
     │  slate "orders"       │                           │
     │──────────────────────►│                           │
     │                       │                           │
     │                       │   [Reader] observe new    │
     │                       │   registration            │
     │                       │◄──────────────────────────│
     │                       │                           │
     │                       │                      create infra
     │                       │                           │
     │                       │   [Writer] update status  │
     │                       │   "provisioned"           │
     │                       │◄──────────────────────────│
     │                       │                           │
     │  [Reader] observe     │                           │
     │  status change        │                           │
     │◄──────────────────────│                           │
     │                       │                           │
```

This is not a formal provisioning protocol—specific workflows are left for future work. The key insight is that the catalog's storage layer *is* the communication channel. There is no separate messaging system or service API.

#### Implications

- **No always-on service** — The catalog does not require a continuously running process. Components open Reader or Writer handles as needed.
- **Distributed coordination via storage** — Object storage (S3, GCS, etc.) serves as the durable communication medium.
- **Consistency from SlateDB** — SlateDB's single-writer guarantee ensures catalog mutations are serialized. Readers see a consistent snapshot.
- **Polling for changes** — Readers must poll to observe updates. Future work may explore change notification mechanisms built on SlateDB.

_Additional design sections to be completed in a future revision._

## Alternatives

_To be completed in a future revision._

## Open Questions

1. **Catalog bootstrap** — How is the catalog itself discovered? If the catalog is a slate, where is its object storage configuration stored?

2. **Bucket metadata** — What specific fields are required for object storage configuration? (bucket name, region, path prefix, credentials reference?)

3. **Catalog location** — Should there be one catalog per bucket, per region, or per "environment"? What is the deployment topology?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-21 | Initial draft |
