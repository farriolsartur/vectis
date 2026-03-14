# Join Configuration Reference

This document provides the complete YAML configuration reference for
stream joins in Vectis.

## Basic Structure

```yaml
joiners:
  - name: my_joiner           # Instance name
    type: my_joiner_type      # Registered joiner type
    config:                   # Component-specific config
      key: value
    join:                     # Join configuration (required)
      correlation_key_path: order_id
      sources: [source1, source2]
```

## Join Configuration Fields

### correlation_key_path (required)

Path to extract the correlation key from each message payload.

**Simple key:**
```yaml
correlation_key_path: order_id
```

**Nested key (dot notation):**
```yaml
correlation_key_path: order.details.id
```

**JSONPath expression:** (requires `jsonpath-ng` package)
```yaml
correlation_key_path: $.order.details.id
```

### sources (required)

List of source component names that will send messages to this joiner.

```yaml
sources:
  - order_source
  - customer_source
  - inventory_source
```

The joiner validates that connections match the declared sources.

### mode (optional)

Join mode determining when to emit results.

| Value | Description | Default |
|-------|-------------|---------|
| `inner` | All sources required | Yes |
| `left_outer` | Primary source required | No |
| `full_outer` | Emit on timeout | No |

```yaml
mode: inner
```

### primary_source (conditional)

Required when `mode: left_outer`. Must be one of the declared sources.

```yaml
mode: left_outer
sources: [orders, customers]
primary_source: orders
```

### window_seconds (optional)

Time window in seconds to wait for matching messages.

| Default | Range |
|---------|-------|
| 30.0 | > 0 |

```yaml
window_seconds: 60.0
```

### max_pending_keys (optional)

Maximum number of correlation keys to buffer simultaneously.

| Default | Range |
|---------|-------|
| 10,000 | >= 1 |

```yaml
max_pending_keys: 50000
```

### max_messages_per_key (optional)

Maximum messages per source per correlation key.

| Default | Range |
|---------|-------|
| 100 | >= 1 |

```yaml
max_messages_per_key: 10
```

### key_ttl_seconds (optional)

Time-to-live for correlation keys. Expired keys are evicted.

| Default | Range |
|---------|-------|
| 60.0 | > 0 |

```yaml
key_ttl_seconds: 120.0
```

### eviction_policy (optional)

Policy when buffer reaches `max_pending_keys`.

| Value | Behavior |
|-------|----------|
| `drop_oldest` | Evict oldest key, emit as partial (default) |
| `drop_newest` | Reject new messages for new keys |
| `error` | Raise MemoryError |

```yaml
eviction_policy: drop_oldest
```

### eos_action (optional)

Action when all sources send END_OF_STREAM.

| Value | Behavior |
|-------|----------|
| `emit_partial` | Emit all pending (some may be incomplete) (default) |
| `drop_incomplete` | Only emit if join condition satisfied |
| `error` | Raise error if incomplete joins remain |

```yaml
eos_action: emit_partial
```

## Complete Example

```yaml
global:
  name: order-enrichment-pipeline
  version: "1.0"

data_providers:
  - name: order_source
    type: order_api_provider
    config:
      endpoint: "http://orders.api/stream"

  - name: customer_source
    type: customer_api_provider
    config:
      endpoint: "http://customers.api/stream"

  - name: inventory_source
    type: inventory_api_provider
    config:
      endpoint: "http://inventory.api/stream"

joiners:
  - name: order_enricher
    type: order_enricher
    config:
      output_format: json
    join:
      correlation_key_path: "$.order_id"
      mode: inner
      sources:
        - order_source
        - customer_source
        - inventory_source
      window_seconds: 30.0
      max_pending_keys: 50000
      max_messages_per_key: 10
      key_ttl_seconds: 60.0
      eviction_policy: drop_oldest
      eos_action: emit_partial

algorithms:
  - name: enriched_processor
    type: order_processor

connections:
  - source: order_source
    targets: [order_enricher]
  - source: customer_source
    targets: [order_enricher]
  - source: inventory_source
    targets: [order_enricher]
  - source: order_enricher
    targets: [enriched_processor]
```

## Validation Rules

1. **sources must be unique**: No duplicate source names
2. **sources must match connections**: All declared sources must have connections
3. **primary_source required for left_outer**: Must specify primary
4. **primary_source must be in sources**: The primary must be declared

## Installation

For JSONPath support, install the optional dependency:

```bash
pip install vectis[joins]
```

Or install directly:

```bash
pip install jsonpath-ng
```
