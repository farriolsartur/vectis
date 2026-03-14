# Custom Component Type Example

This example demonstrates how to add a new component type to Vectis.

## Overview

This example shows:
- **Processor**: A new component type that both receives and sends data
- **MultiplierProcessor**: Custom type that multiplies incoming values
- **CounterProvider**: Generates integers
- **PrinterAlgorithm**: Prints received values
- **Custom YAML Section**: `processors:` for the new type

## Files

- `components.py` - Component type registration + component definitions
- `pipeline.yaml` - Pipeline configuration using `processors:`
- `run.py` - Entry point script

## Running

```bash
# From the project root
python -m examples.custom_component_type.run

# Or directly
python examples/custom_component_type/run.py
```

## Pipeline Flow

```
Counter (DataProvider)
        |
        v
Multiplier (Processor)  <-- custom component type
        |
        v
Printer (Algorithm)
```

## Key Concepts

### Registering a New Component Type

```python
_type_registry = get_component_type_registry()
_type_registry.register_type("processor", Processor)
processor = _type_registry.create_decorator("processor")
```

This lets you use a new top-level YAML section:

```yaml
processors:
  - name: multiplier
    type: value_multiplier
    config:
      factor: 3
```

### Processor Pattern

Processors both receive and send messages by mixing in
`ProcessorMixin` (Sender + Receiver):

```python
@processor("value_multiplier")
class MultiplierProcessor(Processor[MultiplyConfig]):
    async def on_received_data(self, message: Message[Any]) -> None:
        result = message.payload["value"] * self.config.factor
        await self.send_data({"value": result})
```

## Expected Output

```
Vectis Custom Component Type Example
============================================================
Config: examples/custom_component_type/pipeline.yaml

Pipeline: Counter -> Multiplier (processor) -> Printer
  - Counter generates 5 integers starting at 1
  - Multiplier is a custom component type (processor)
  - Printer receives multiplied values

[printer] Starting...
[printer] Received #1: {'value': 3}
[printer] Received #2: {'value': 6}
[printer] Received #3: {'value': 9}
[printer] Received #4: {'value': 12}
[printer] Received #5: {'value': 15}
[printer] Stopped. Total received: 5

============================================================
Pipeline Results
============================================================
Counter sent:     [1, 2, 3, 4, 5]
Multiplier output: [3, 6, 9, 12, 15]
Printer received: [3, 6, 9, 12, 15]
```

## Next Steps

- See `simple_pipeline` for the simplest pipeline setup
- See `etl_pipeline` for component chaining
- See `distributed_example` for multi-worker deployment
