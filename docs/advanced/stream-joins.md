# Stream Joins

Stream joins allow you to correlate messages from multiple upstream sources
based on a shared key. This is essential for patterns like:

- **API enrichment**: Join order data with customer profiles
- **Event correlation**: Match events from different systems
- **Data reconciliation**: Compare records across databases

## Concepts

### Join Correlation

A join correlates messages from multiple sources by extracting a **correlation key**
from each message's payload. When messages from all (or some) sources share the
same key, they are "joined" and processed together.

```
Source A ──▶ {order_id: "123", ...}  ─┐
                                      │  Join on
Source B ──▶ {order_id: "123", ...}  ─┼─▶ order_id ──▶ Combined Result
                                      │
Source C ──▶ {order_id: "123", ...}  ─┘
```

### Join Modes

Vectis supports three join modes:

| Mode | Emit Condition | Use Case |
|------|----------------|----------|
| **INNER** | All sources have data | Strict correlation, all data required |
| **LEFT_OUTER** | Primary source has data | Optional enrichment, primary always emits |
| **FULL_OUTER** | On timeout/EOS | Best-effort matching, emit whatever available |

#### Inner Join

The default mode. Waits until ALL declared sources have at least one message
for a correlation key before emitting.

```yaml
join:
  mode: inner
  sources: [orders, customers, inventory]
```

#### Left Outer Join

Emits when the **primary source** has data. Other sources are optional.
Useful when you want to enrich data but can proceed without all enrichment.

```yaml
join:
  mode: left_outer
  sources: [orders, customers]
  primary_source: orders  # Required for left_outer
```

#### Full Outer Join

Emits on timeout or end-of-stream with whatever data is available.
Useful for best-effort matching where some data may never arrive.

```yaml
join:
  mode: full_outer
  sources: [orders, customers]
  window_seconds: 5.0  # Emit after 5 seconds
```

### Memory Management

Stream joins buffer messages while waiting for matches. Vectis provides
controls to prevent unbounded memory growth:

- **max_pending_keys**: Maximum correlation keys to track simultaneously
- **max_messages_per_key**: Maximum messages per source per key
- **key_ttl_seconds**: Automatic expiration of old correlation keys
- **eviction_policy**: What to do when buffer is full

### End-of-Stream Handling

When upstream sources send END_OF_STREAM, the joiner must decide what to do
with pending incomplete joins:

| EOS Action | Behavior |
|------------|----------|
| **emit_partial** | Emit all pending keys (may be incomplete) |
| **drop_incomplete** | Only emit keys that satisfy join condition |
| **error** | Raise error if incomplete joins remain |

## The Joiner Component

### Creating a Joiner

```python
from vectis.components import Joiner, joiner

@joiner("order_enricher")
class OrderEnricher(Joiner[MyConfig]):
    async def on_joined(self, key, messages):
        """Called when join is complete according to mode."""
        order = messages["orders"][0].payload
        customer = messages["customers"][0].payload

        enriched = {"order": order, "customer": customer}
        await self.send_data(enriched)
```

### Required Methods

| Method | Description |
|--------|-------------|
| `on_joined(key, messages)` | Called when join condition is satisfied |

### Optional Overrides

| Method | Default Behavior |
|--------|-----------------|
| `on_partial_join(key, messages, reason)` | Logs warning and drops |
| `on_received_error(message)` | Logs error |
| `on_start()` | Starts timeout loop |
| `on_stop()` | Flushes pending, stops loop |

### The `messages` Parameter

The `messages` dict maps source names to lists of messages:

```python
async def on_joined(self, key, messages):
    # messages = {
    #     "source_a": [Message, Message, ...],
    #     "source_b": [Message, ...],
    #     ...
    # }
    pass
```

Multiple messages per source can occur if the same key appears multiple times.

## Configuration Reference

See [Join Configuration](join-configuration.md) for complete YAML reference.

## Common Patterns

See [Join Patterns](join-patterns.md) for implementation patterns.
