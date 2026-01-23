# RFC 0004: HTTP APIs

**Status**: Draft 

**Authors**:
- [Apurva Mehta](https://github.com/apurvam)

## Summary

This RFC introduce HTTP APIs for the OpenData-Log, enabling users to append and scan events using HTTP with JSON 
payloads.

## Motivation

A native HTTP API will make OpenData-Log maximally accessible to developers, enabling access to the long in any language
and programming framework. By being the most accessible protocol and by supporting a text payload format, the HTTP API
will improve the out-of-the-box developer experience. It does not preclude more performant binary APIs from being added
later.

## Goals

- Add a basic HTTP API with a JSON payload

## Non-Goals

- Supporting binary keys and value via HTTP.
- Non-HTTP APIs over protocols like GRPC, etc.

## Design

### Log HTTP Server API Summary

Endpoints
┌──────────────────────┬────────┬────────────────────────────────────────────┐
│       Endpoint       │ Method │                Description                 │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /api/v1/log/append   │ POST   │ Append records to the log                  │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /api/v1/log/scan     │ GET    │ Scan entries by key and sequence range     │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /api/v1/log/keys     │ GET    │ List distinct keys within a segment range  │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /api/v1/log/segments │ GET    │ List segments overlapping a sequence range │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /api/v1/log/count    │ GET    │ Count entries for a key                    │
├──────────────────────┼────────┼────────────────────────────────────────────┤
│ /metrics             │ GET    │ Prometheus metrics                         │
└──────────────────────┴────────┴────────────────────────────────────────────┘

### APIs 

#### Append
`POST /api/v1/log/append`

Append records to the log.

Request Body:
```json 
{
"records": [
   { "key": "my-key", "value": "my-value" }
],
"await_durable": false
}
```

Response:
```json
{ "status": "success", "records_appended": 1 }
```

#### Scan

`GET /api/v1/log/scan`

Scan entries for a specific key within a sequence range.

Query Parameters:
┌───────────┬────────┬──────────┬─────────────────────────────────────────────┐
│   Param   │  Type  │ Required │                 Description                 │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ key       │ string │ yes      │ Key to scan                                 │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ start_seq │ u64    │ no       │ Start sequence (inclusive), default: 0      │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ end_seq   │ u64    │ no       │ End sequence (exclusive), default: u64::MAX │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ limit     │ usize  │ no       │ Max entries to return, default: 1000        │
└───────────┴────────┴──────────┴─────────────────────────────────────────────┘

Response:

```json
{
  "status": "success",
  "entries": [
    { "key": "my-key", "sequence": 0, "value": "my-value" }
  ]
} 
```


  ---
GET /api/v1/log/segments

List segments overlapping a sequence range. Use this to discover segment boundaries before calling /keys.

Query Parameters:
┌───────────┬──────┬──────────┬─────────────────────────────────────────────┐
│   Param   │ Type │ Required │                 Description                 │
├───────────┼──────┼──────────┼─────────────────────────────────────────────┤
│ start_seq │ u64  │ no       │ Start sequence (inclusive), default: 0      │
├───────────┼──────┼──────────┼─────────────────────────────────────────────┤
│ end_seq   │ u64  │ no       │ End sequence (exclusive), default: u64::MAX │
└───────────┴──────┴──────────┴─────────────────────────────────────────────┘
Response:
{
"status": "success",
"segments": [
{ "id": 0, "start_seq": 0, "start_time_ms": 1705766400000 },
{ "id": 1, "start_seq": 100, "start_time_ms": 1705766460000 }
]
}

  ---
GET /api/v1/log/keys

List distinct keys within a segment range.

Query Parameters:
┌───────────────┬───────┬──────────┬───────────────────────────────────────────────┐
│     Param     │ Type  │ Required │                  Description                  │
├───────────────┼───────┼──────────┼───────────────────────────────────────────────┤
│ start_segment │ u32   │ no       │ Start segment ID (inclusive), default: 0      │
├───────────────┼───────┼──────────┼───────────────────────────────────────────────┤
│ end_segment   │ u32   │ no       │ End segment ID (exclusive), default: u32::MAX │
├───────────────┼───────┼──────────┼───────────────────────────────────────────────┤
│ limit         │ usize │ no       │ Max keys to return, default: 1000             │
└───────────────┴───────┴──────────┴───────────────────────────────────────────────┘
Response:
{
"status": "success",
"keys": [
{ "key": "events" },
{ "key": "orders" }
]
}

  ---
GET /api/v1/log/count

Count entries for a key within a sequence range.

Query Parameters:
┌───────────┬────────┬──────────┬─────────────────────────────────────────────┐
│   Param   │  Type  │ Required │                 Description                 │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ key       │ string │ yes      │ Key to count                                │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ start_seq │ u64    │ no       │ Start sequence (inclusive), default: 0      │
├───────────┼────────┼──────────┼─────────────────────────────────────────────┤
│ end_seq   │ u64    │ no       │ End sequence (exclusive), default: u64::MAX │
└───────────┴────────┴──────────┴─────────────────────────────────────────────┘
Response:
{ "status": "success", "count": 42 }



```rust
// Example code blocks for API proposals
struct Example {
    field: Type,
}
```

## Alternatives

What other approaches were considered? Why were they rejected? This helps
readers understand the design space and the reasoning behind the chosen
approach.

## Open Questions

Optional section for unresolved questions that need input during the review
process.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-22 | Initial draft |
