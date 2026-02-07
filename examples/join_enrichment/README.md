# Order Enrichment Example

This example demonstrates **stream joins** in FlowForge. It shows how to correlate
data from multiple upstream sources based on a shared key.

## Overview

The pipeline simulates an order processing system where:

1. **Order Source** - Streams order data (product, quantity, price)
2. **Customer Source** - Streams customer data (name, email, tier)
3. **Inventory Source** - Streams inventory data (stock, warehouse)

A **Joiner** component correlates these three streams by `order_id`, waiting
until all three sources have data for a given order before emitting an
enriched order document.

## Architecture

```
┌─────────────────┐
│  Order Source   │──┐
└─────────────────┘  │
                     │
┌─────────────────┐  │    ┌─────────────────┐    ┌─────────────────┐
│ Customer Source │──┼───▶│ Order Enricher  │───▶│   Processor     │
└─────────────────┘  │    │    (Joiner)     │    │  (Algorithm)    │
                     │    └─────────────────┘    └─────────────────┘
┌─────────────────┐  │
│Inventory Source │──┘
└─────────────────┘
```

## Running the Example

From the `examples/join_enrichment` directory:

```bash
python run.py
```

## Join Configuration

The joiner is configured in `pipeline.yaml`:

```yaml
joiners:
  - name: order_enricher
    type: order_enricher
    join:
      # Key extraction path
      correlation_key_path: order_id

      # Join type
      mode: inner

      # Expected sources
      sources:
        - order_source
        - customer_source
        - inventory_source

      # Timing
      window_seconds: 30.0
      key_ttl_seconds: 60.0

      # Memory management
      max_pending_keys: 10000
      eviction_policy: drop_oldest
      eos_action: emit_partial
```

## Join Modes

FlowForge supports three join modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| `inner` | Emit only when ALL sources have data | Strict correlation |
| `left_outer` | Emit when primary source has data | Optional enrichment |
| `full_outer` | Emit on timeout with any data | Best-effort matching |

## Key Concepts

### Correlation Key Extraction

The `correlation_key_path` supports:

- Simple keys: `order_id`
- Nested keys: `order.details.id`
- JSONPath: `$.order.details.id` (requires `jsonpath-ng`)

### Memory Management

Stream joins can accumulate memory waiting for matching messages. FlowForge
provides controls to prevent unbounded growth:

- `max_pending_keys`: Maximum correlation keys to buffer
- `max_messages_per_key`: Maximum messages per source per key
- `key_ttl_seconds`: Automatic expiration of old keys
- `eviction_policy`: How to handle overflow (`drop_oldest`, `drop_newest`, `error`)

### End-of-Stream Handling

When sources end, pending incomplete joins are handled based on `eos_action`:

- `emit_partial`: Emit all pending keys (some may be incomplete)
- `drop_incomplete`: Only emit keys that would have been complete
- `error`: Raise error if incomplete joins remain

## Implementing a Joiner

```python
from flowforge.components import Joiner, joiner

@joiner("my_enricher")
class MyEnricher(Joiner[MyConfig]):
    async def on_joined(self, key, messages):
        """Called when join is complete."""
        order = messages["orders"][0].payload
        customer = messages["customers"][0].payload

        enriched = {
            "order_id": key,
            "order": order,
            "customer": customer,
        }
        await self.send_data(enriched)

    async def on_partial_join(self, key, messages, reason):
        """Called for incomplete joins (optional override)."""
        print(f"Partial join for {key}: {reason}")
```

## Expected Output

```
============================================================
Order Enrichment Pipeline - Stream Joins Demo
============================================================

This pipeline demonstrates FlowForge stream joins:
  - 3 data providers: orders, customers, inventory
  - 1 joiner: correlates by order_id
  - 1 processor: handles enriched orders

------------------------------------------------------------
Processing orders...
------------------------------------------------------------
  [1] Order ORD-0000: Alice ordered 1x PROD-000 ($10.00) - Fulfillable
  [2] Order ORD-0001: Bob ordered 2x PROD-001 ($22.00) - Fulfillable
  ...

  Summary: Processed 10 orders, 8 fulfillable

------------------------------------------------------------
Pipeline Statistics
------------------------------------------------------------
  Joined orders:  10
  Partial joins:  0
  Processed:      10
  Fulfillable:    8

============================================================
Pipeline completed successfully!
============================================================
```
