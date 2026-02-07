# Join Patterns

This guide covers common patterns for implementing stream joins in FlowForge.

## API Enrichment

The most common join pattern: enriching a primary data stream with data from
lookup APIs.

### Pattern

```
Orders API ────────┐
                   │
Customer API ──────┼───▶ Joiner ───▶ Enriched Orders
                   │
Inventory API ─────┘
```

### Implementation

```python
@joiner("order_enricher")
class OrderEnricher(Joiner[EnricherConfig]):
    async def on_joined(self, key, messages):
        order = messages["orders"][0].payload
        customer = messages["customers"][0].payload
        inventory = messages["inventory"][0].payload

        enriched = {
            "order_id": key,
            "order": order,
            "customer": {
                "name": customer["name"],
                "email": customer["email"],
            },
            "inventory": {
                "in_stock": inventory["quantity"] > order["quantity"],
            },
        }
        await self.send_data(enriched)
```

### Configuration

```yaml
join:
  correlation_key_path: order_id
  mode: inner
  sources: [orders, customers, inventory]
  window_seconds: 10.0
```

## Optional Enrichment (Left Outer)

When enrichment data is nice-to-have but not required.

### Pattern

```
Orders ─────▶┐
             │
Customers ──▶├───▶ Joiner ───▶ Orders (with optional customer data)
             │
```

### Implementation

```python
@joiner("optional_enricher")
class OptionalEnricher(Joiner[Config]):
    async def on_joined(self, key, messages):
        order = messages["orders"][0].payload

        # Customer data may or may not be present
        customer_msgs = messages.get("customers", [])
        customer = customer_msgs[0].payload if customer_msgs else None

        enriched = {
            "order_id": key,
            "order": order,
            "customer": customer,  # May be None
        }
        await self.send_data(enriched)
```

### Configuration

```yaml
join:
  correlation_key_path: order_id
  mode: left_outer
  sources: [orders, customers]
  primary_source: orders
```

## Event Correlation

Correlating events from different systems that share a transaction ID.

### Pattern

```
Payment Events ────┐
                   │
Shipping Events ───┼───▶ Joiner ───▶ Correlated Transaction
                   │
Notification Events ┘
```

### Implementation

```python
@joiner("event_correlator")
class EventCorrelator(Joiner[Config]):
    async def on_joined(self, key, messages):
        events = {}
        for source, msgs in messages.items():
            events[source] = [m.payload for m in msgs]

        correlated = {
            "transaction_id": key,
            "events": events,
            "complete": len(messages) == len(self._join_config.sources),
        }
        await self.send_data(correlated)

    async def on_partial_join(self, key, messages, reason):
        # Emit partial correlation on timeout
        events = {source: [m.payload for m in msgs] for source, msgs in messages.items()}
        await self.send_data({
            "transaction_id": key,
            "events": events,
            "complete": False,
            "reason": reason,
        })
```

### Configuration

```yaml
join:
  correlation_key_path: transaction_id
  mode: full_outer
  sources: [payments, shipping, notifications]
  window_seconds: 60.0
  eos_action: emit_partial
```

## Data Reconciliation

Comparing records across systems to find discrepancies.

### Pattern

```
System A Records ───┐
                    │
System B Records ───┼───▶ Joiner ───▶ Reconciliation Report
                    │
```

### Implementation

```python
@joiner("reconciler")
class Reconciler(Joiner[Config]):
    async def on_joined(self, key, messages):
        record_a = messages.get("system_a", [None])[0]
        record_b = messages.get("system_b", [None])[0]

        # Compare records
        discrepancies = self._compare(
            record_a.payload if record_a else None,
            record_b.payload if record_b else None,
        )

        await self.send_data({
            "key": key,
            "system_a": record_a.payload if record_a else None,
            "system_b": record_b.payload if record_b else None,
            "discrepancies": discrepancies,
            "status": "match" if not discrepancies else "mismatch",
        })

    async def on_partial_join(self, key, messages, reason):
        # Report missing records
        present_in = list(messages.keys())
        missing_from = [
            s for s in self._join_config.sources if s not in present_in
        ]

        await self.send_data({
            "key": key,
            "status": "missing",
            "present_in": present_in,
            "missing_from": missing_from,
        })

    def _compare(self, a, b):
        if a is None or b is None:
            return ["missing_record"]

        discrepancies = []
        for field in ["amount", "status", "date"]:
            if a.get(field) != b.get(field):
                discrepancies.append({
                    "field": field,
                    "system_a": a.get(field),
                    "system_b": b.get(field),
                })
        return discrepancies
```

### Configuration

```yaml
join:
  correlation_key_path: record_id
  mode: full_outer
  sources: [system_a, system_b]
  window_seconds: 5.0
  eos_action: emit_partial
```

## Multi-Stage Enrichment

Chaining joins for complex enrichment pipelines.

### Pattern

```
Orders ─────┐
            ├───▶ Stage1 ─────┐
Customers ──┘                 │
                              ├───▶ Stage2 ───▶ Fully Enriched
Inventory ────────────────────┘
```

### Implementation

```python
# First joiner: Orders + Customers
@joiner("stage1_enricher")
class Stage1Enricher(Joiner[Config]):
    async def on_joined(self, key, messages):
        order = messages["orders"][0].payload
        customer = messages["customers"][0].payload

        # Emit intermediate result with key preserved
        await self.send_data({
            "order_id": key,  # Preserve key for next stage
            "order": order,
            "customer": customer,
        })


# Second joiner: Stage1 output + Inventory
@joiner("stage2_enricher")
class Stage2Enricher(Joiner[Config]):
    async def on_joined(self, key, messages):
        stage1 = messages["stage1"][0].payload
        inventory = messages["inventory"][0].payload

        await self.send_data({
            "order_id": key,
            "order": stage1["order"],
            "customer": stage1["customer"],
            "inventory": inventory,
        })
```

### Configuration

```yaml
joiners:
  - name: stage1
    type: stage1_enricher
    join:
      correlation_key_path: order_id
      sources: [orders, customers]

  - name: stage2
    type: stage2_enricher
    join:
      correlation_key_path: order_id
      sources: [stage1, inventory]

connections:
  - source: orders
    targets: [stage1]
  - source: customers
    targets: [stage1]
  - source: stage1
    targets: [stage2]
  - source: inventory
    targets: [stage2]
```

## Handling Duplicates

When the same key may appear multiple times from a source.

### Implementation

```python
@joiner("dedup_enricher")
class DedupEnricher(Joiner[Config]):
    async def on_joined(self, key, messages):
        # Take only the most recent message from each source
        latest = {}
        for source, msgs in messages.items():
            # Messages are ordered by arrival time
            latest[source] = msgs[-1].payload

        await self.send_data({
            "key": key,
            **latest,
        })
```

### Configuration

```yaml
join:
  max_messages_per_key: 10  # Buffer up to 10 duplicates per source
```

## Error Handling

Handling errors in join processing.

### Implementation

```python
@joiner("safe_enricher")
class SafeEnricher(Joiner[Config]):
    async def on_joined(self, key, messages):
        try:
            result = self._process(key, messages)
            await self.send_data(result)
        except Exception as e:
            await self.send_error(f"Join failed for {key}: {e}")

    async def on_partial_join(self, key, messages, reason):
        # Log and optionally retry or escalate
        logger.warning("Partial join: key=%s, reason=%s", key, reason)

        if reason == "ttl":
            # Key expired, emit warning message
            await self.send_data({
                "key": key,
                "status": "timeout",
                "available_sources": list(messages.keys()),
            })
```

## Best Practices

1. **Keep correlation keys stable**: Use immutable identifiers (order_id, transaction_id)

2. **Set appropriate timeouts**: Balance between completeness and latency

3. **Monitor buffer usage**: Track `pending_key_count` for capacity planning

4. **Handle partial joins explicitly**: Don't ignore `on_partial_join`

5. **Preserve keys for downstream**: Include correlation key in output for debugging

6. **Use appropriate join mode**: Don't use INNER when LEFT_OUTER suffices

7. **Test with realistic timing**: Sources may arrive at different rates
