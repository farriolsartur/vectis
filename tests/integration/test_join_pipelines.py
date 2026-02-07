"""Integration tests for FlowForge stream join pipelines.

These tests verify end-to-end pipeline execution with joiner components,
including multi-source joins, timeout handling, and EOS behavior.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from flowforge import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
    get_component_registry,
    get_component_type_registry,
)
from flowforge.components import (
    JoinConfig,
    JoinMode,
    Joiner,
    joiner,
)
from flowforge.engine.engine import Engine


# =============================================================================
# Test Configurations
# =============================================================================


class OrderProviderConfig(BaseModel):
    """Configuration for order data provider."""

    orders: list[dict[str, Any]]
    delay: float = 0.0


class CustomerProviderConfig(BaseModel):
    """Configuration for customer data provider."""

    customers: list[dict[str, Any]]
    delay: float = 0.0


class EnricherConfig(BaseModel):
    """Configuration for order enricher."""

    output_format: str = "dict"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    # Re-register built-in types
    from flowforge.components.types import _register_builtin_types

    _register_builtin_types()
    yield
    get_component_registry().clear()


@pytest.fixture
def register_join_components():
    """Register components for join pipeline tests."""

    @data_provider("order_provider")
    class OrderProvider(DataProvider[OrderProviderConfig]):
        async def run(self) -> None:
            for order in self.config.orders:
                if self._stop_requested:
                    break
                await self.send_data(order)
                if self.config.delay > 0:
                    await asyncio.sleep(self.config.delay)
            await self.send_end_of_stream()

    @data_provider("customer_provider")
    class CustomerProvider(DataProvider[CustomerProviderConfig]):
        async def run(self) -> None:
            for customer in self.config.customers:
                if self._stop_requested:
                    break
                await self.send_data(customer)
                if self.config.delay > 0:
                    await asyncio.sleep(self.config.delay)
            await self.send_end_of_stream()

    @joiner("order_enricher")
    class OrderEnricher(Joiner[EnricherConfig]):
        async def on_joined(
            self,
            key: Any,
            messages: dict[str, list[Message[Any]]],
        ) -> None:
            # Get first message from each source
            order = messages.get("order_source", [None])[0]
            customer = messages.get("customer_source", [None])[0]

            if order and customer:
                enriched = {
                    "order_id": key,
                    "order": order.payload,
                    "customer": customer.payload,
                }
                await self.send_data(enriched)

    @algorithm("result_collector")
    class ResultCollector(Algorithm[EmptyConfig]):
        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.results: list[dict[str, Any]] = []

        async def on_received_data(self, message: Message[Any]) -> None:
            self.results.append(message.payload)

    return {
        "OrderProvider": OrderProvider,
        "CustomerProvider": CustomerProvider,
        "OrderEnricher": OrderEnricher,
        "ResultCollector": ResultCollector,
    }


# =============================================================================
# Helper Functions
# =============================================================================


def create_join_pipeline_yaml(
    orders: list[dict[str, Any]],
    customers: list[dict[str, Any]],
    join_mode: str = "inner",
    primary_source: str | None = None,
) -> str:
    """Create YAML configuration for a join pipeline."""
    primary_line = f"\n      primary_source: {primary_source}" if primary_source else ""

    return f"""
global:
  name: join-test-pipeline
  version: "1.0"
  defaults:
    distribution: fan_out

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: {orders}
  - name: customer_source
    type: customer_provider
    config:
      customers: {customers}

joiners:
  - name: enricher
    type: order_enricher
    config:
      output_format: dict
    join:
      correlation_key_path: order_id
      mode: {join_mode}
      sources: [order_source, customer_source]{primary_line}
      window_seconds: 5.0
      max_pending_keys: 1000

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""


# =============================================================================
# Integration Tests
# =============================================================================


class TestBasicJoinPipeline:
    """Tests for basic join pipeline functionality."""

    @pytest.mark.asyncio
    async def test_inner_join_matching_keys(self, register_join_components):
        """Test inner join with matching keys from both sources."""
        orders = [
            {"order_id": "1", "product": "Widget"},
            {"order_id": "2", "product": "Gadget"},
        ]
        customers = [
            {"order_id": "1", "name": "Alice"},
            {"order_id": "2", "name": "Bob"},
        ]

        yaml_content = create_join_pipeline_yaml(orders, customers)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None
            assert len(collector.results) == 2

            # Check that orders are enriched with customer data
            results_by_id = {r["order_id"]: r for r in collector.results}
            assert "1" in results_by_id
            assert "2" in results_by_id
            assert results_by_id["1"]["customer"]["name"] == "Alice"
            assert results_by_id["2"]["customer"]["name"] == "Bob"
        finally:
            Path(config_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_inner_join_partial_match(self, register_join_components):
        """Test inner join where some keys don't have matches."""
        orders = [
            {"order_id": "1", "product": "Widget"},
            {"order_id": "2", "product": "Gadget"},
            {"order_id": "3", "product": "Thing"},
        ]
        customers = [
            {"order_id": "1", "name": "Alice"},
            {"order_id": "3", "name": "Charlie"},
        ]

        yaml_content = create_join_pipeline_yaml(orders, customers)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None

            # Only orders with matching customers should be emitted
            # Order 2 has no matching customer, so it should not appear
            # But with emit_partial on EOS, incomplete joins may be emitted
            # depending on the EOS action
            results_by_id = {r["order_id"]: r for r in collector.results}

            # Orders 1 and 3 have matches
            assert "1" in results_by_id
            assert "3" in results_by_id
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestLeftOuterJoin:
    """Tests for left outer join functionality."""

    @pytest.mark.asyncio
    async def test_left_outer_join_emits_on_primary(self, register_join_components):
        """Test that left outer join emits when primary source has data.

        In LEFT_OUTER mode, once the primary source data arrives for a key,
        the join is considered complete. This means:
        - If customer data arrives BEFORE the order, it waits for the order
        - If order data arrives first, it emits immediately (customer may be None)
        - If both arrive "together" (close enough), customer will be present

        This test verifies that orders are emitted even when customer data
        is not available (the LEFT OUTER semantic).
        """
        # Register left outer specific joiner
        @joiner("left_outer_enricher")
        class LeftOuterEnricher(Joiner[EnricherConfig]):
            async def on_joined(
                self,
                key: Any,
                messages: dict[str, list[Message[Any]]],
            ) -> None:
                order = messages.get("order_source", [None])[0]
                customer_msgs = messages.get("customer_source", [])
                customer = customer_msgs[0] if customer_msgs else None

                if order:
                    enriched = {
                        "order_id": key,
                        "order": order.payload,
                        "customer": customer.payload if customer else None,
                    }
                    await self.send_data(enriched)

        # Send customers FIRST with a delay, then orders
        # This ensures customer data is in the buffer when order arrives
        @data_provider("delayed_order_provider")
        class DelayedOrderProvider(DataProvider[OrderProviderConfig]):
            async def run(self) -> None:
                # Wait for customers to be sent first
                await asyncio.sleep(0.1)
                for order in self.config.orders:
                    if self._stop_requested:
                        break
                    await self.send_data(order)
                await self.send_end_of_stream()

        orders = [
            {"order_id": "1", "product": "Widget"},
            {"order_id": "2", "product": "Gadget"},
        ]
        customers = [
            {"order_id": "1", "name": "Alice"},
        ]

        yaml_content = f"""
global:
  name: left-outer-join-test
  version: "1.0"

data_providers:
  - name: order_source
    type: delayed_order_provider
    config:
      orders: {orders}
  - name: customer_source
    type: customer_provider
    config:
      customers: {customers}

joiners:
  - name: enricher
    type: left_outer_enricher
    join:
      correlation_key_path: order_id
      mode: left_outer
      sources: [order_source, customer_source]
      primary_source: order_source
      window_seconds: 5.0
      eos_action: emit_partial

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None

            # Both orders should be emitted (left outer join)
            results_by_id = {r["order_id"]: r for r in collector.results}

            # At minimum, both orders should be present
            assert "1" in results_by_id
            assert "2" in results_by_id

            # Order 1 should have customer (customer arrived first)
            assert results_by_id["1"]["customer"] is not None
            assert results_by_id["1"]["customer"]["name"] == "Alice"

            # Order 2 has no matching customer
            assert results_by_id["2"]["customer"] is None
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestThreeWayJoin:
    """Tests for three-way join functionality."""

    @pytest.mark.asyncio
    async def test_three_way_inner_join(self, register_join_components):
        """Test inner join with three sources."""

        @data_provider("inventory_provider")
        class InventoryProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                inventory = [
                    {"order_id": "1", "stock": 100},
                    {"order_id": "2", "stock": 50},
                ]
                for item in inventory:
                    if self._stop_requested:
                        break
                    await self.send_data(item)
                await self.send_end_of_stream()

        @joiner("three_way_enricher")
        class ThreeWayEnricher(Joiner[EmptyConfig]):
            async def on_joined(
                self,
                key: Any,
                messages: dict[str, list[Message[Any]]],
            ) -> None:
                order = messages.get("order_source", [None])[0]
                customer = messages.get("customer_source", [None])[0]
                inventory = messages.get("inventory_source", [None])[0]

                if order and customer and inventory:
                    enriched = {
                        "order_id": key,
                        "order": order.payload,
                        "customer": customer.payload,
                        "inventory": inventory.payload,
                    }
                    await self.send_data(enriched)

        orders = [
            {"order_id": "1", "product": "Widget"},
            {"order_id": "2", "product": "Gadget"},
        ]
        customers = [
            {"order_id": "1", "name": "Alice"},
            {"order_id": "2", "name": "Bob"},
        ]

        yaml_content = f"""
global:
  name: three-way-join-test
  version: "1.0"

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: {orders}
  - name: customer_source
    type: customer_provider
    config:
      customers: {customers}
  - name: inventory_source
    type: inventory_provider

joiners:
  - name: enricher
    type: three_way_enricher
    join:
      correlation_key_path: order_id
      mode: inner
      sources: [order_source, customer_source, inventory_source]
      window_seconds: 5.0

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: inventory_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None
            assert len(collector.results) == 2

            # Both orders should be enriched with all three data sources
            results_by_id = {r["order_id"]: r for r in collector.results}
            assert results_by_id["1"]["customer"]["name"] == "Alice"
            assert results_by_id["1"]["inventory"]["stock"] == 100
            assert results_by_id["2"]["customer"]["name"] == "Bob"
            assert results_by_id["2"]["inventory"]["stock"] == 50
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestJoinConfigValidation:
    """Tests for join configuration validation in pipelines."""

    @pytest.mark.asyncio
    async def test_missing_join_config_fails(self, register_join_components):
        """Test that joiner without join config fails validation."""
        yaml_content = """
global:
  name: missing-join-config-test
  version: "1.0"

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: []

joiners:
  - name: enricher
    type: order_enricher
    config:
      output_format: dict

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            with pytest.raises(Exception) as exc_info:
                await engine.run()
            # Should fail due to missing join config
            assert "join" in str(exc_info.value).lower()
        finally:
            Path(config_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_mismatched_sources_fails(self, register_join_components):
        """Test that mismatched sources in join config fails validation."""
        orders = [{"order_id": "1", "product": "Widget"}]

        yaml_content = f"""
global:
  name: mismatched-sources-test
  version: "1.0"

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: {orders}

joiners:
  - name: enricher
    type: order_enricher
    config:
      output_format: dict
    join:
      correlation_key_path: order_id
      mode: inner
      sources: [order_source, customer_source]

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            with pytest.raises(Exception) as exc_info:
                await engine.run()
            # Should fail due to missing connection from customer_source
            assert "missing" in str(exc_info.value).lower() or "source" in str(
                exc_info.value
            ).lower()
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestFullOuterJoin:
    """Tests for full outer join functionality."""

    @pytest.mark.asyncio
    async def test_full_outer_emits_immediately(self, register_join_components):
        """Test that full outer join emits when any source has data.

        FULL_OUTER semantics: emits as data arrives. This means each key
        emits when the FIRST message arrives for that key (from any source).
        The result may have partial data if sources arrive at different times.
        """

        @joiner("full_outer_enricher")
        class FullOuterEnricher(Joiner[EnricherConfig]):
            async def on_joined(
                self,
                key: Any,
                messages: dict[str, list[Message[Any]]],
            ) -> None:
                order_msgs = messages.get("order_source", [])
                customer_msgs = messages.get("customer_source", [])

                enriched = {
                    "order_id": key,
                    "order": order_msgs[0].payload if order_msgs else None,
                    "customer": customer_msgs[0].payload if customer_msgs else None,
                }
                await self.send_data(enriched)

        # Orders and customers with non-overlapping keys
        orders = [
            {"order_id": "1", "product": "Widget"},
            {"order_id": "2", "product": "Gadget"},
        ]
        customers = [
            {"order_id": "3", "name": "Charlie"},  # Key 3 has no matching order
        ]

        yaml_content = f"""
global:
  name: full-outer-join-test
  version: "1.0"

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: {orders}
  - name: customer_source
    type: customer_provider
    config:
      customers: {customers}

joiners:
  - name: enricher
    type: full_outer_enricher
    join:
      correlation_key_path: order_id
      mode: full_outer
      sources: [order_source, customer_source]
      window_seconds: 5.0
      eos_action: emit_partial

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None

            # FULL_OUTER should emit for ALL unique keys (1, 2, and 3)
            # Each key emits when first message arrives
            results_by_id = {r["order_id"]: r for r in collector.results}

            # All three keys should have results
            assert len(results_by_id) == 3
            assert "1" in results_by_id
            assert "2" in results_by_id
            assert "3" in results_by_id

            # Key 3: has only customer (no matching order)
            assert results_by_id["3"]["order"] is None
            assert results_by_id["3"]["customer"] is not None
            assert results_by_id["3"]["customer"]["name"] == "Charlie"
        finally:
            Path(config_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_full_outer_emits_partial_data(self, register_join_components):
        """Test that full outer join emits partial data immediately."""

        @joiner("partial_full_outer_enricher")
        class PartialFullOuterEnricher(Joiner[EnricherConfig]):
            async def on_joined(
                self,
                key: Any,
                messages: dict[str, list[Message[Any]]],
            ) -> None:
                order_msgs = messages.get("order_source", [])
                customer_msgs = messages.get("customer_source", [])

                enriched = {
                    "order_id": key,
                    "has_order": len(order_msgs) > 0,
                    "has_customer": len(customer_msgs) > 0,
                }
                await self.send_data(enriched)

        # Single order, no customers
        orders = [{"order_id": "solo", "product": "Widget"}]
        customers: list[dict[str, Any]] = []

        yaml_content = f"""
global:
  name: full-outer-partial-test
  version: "1.0"

data_providers:
  - name: order_source
    type: order_provider
    config:
      orders: {orders}
  - name: customer_source
    type: customer_provider
    config:
      customers: {customers}

joiners:
  - name: enricher
    type: partial_full_outer_enricher
    join:
      correlation_key_path: order_id
      mode: full_outer
      sources: [order_source, customer_source]
      window_seconds: 5.0

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None

            # Should emit for the solo order even without customer
            assert len(collector.results) >= 1

            result = next(
                (r for r in collector.results if r["order_id"] == "solo"), None
            )
            assert result is not None
            assert result["has_order"] is True
            assert result["has_customer"] is False
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestEOSPropagation:
    """Tests for EOS propagation through joiner pipelines."""

    @pytest.mark.asyncio
    async def test_joiner_propagates_eos_downstream(self, register_join_components):
        """Test that the joiner sends EOS downstream after all sources end.

        Uses a custom collector that tracks whether on_received_ending was
        called, verifying the joiner properly propagates EOS through the
        MultiplexInputChannel.
        """

        @algorithm("eos_tracking_collector")
        class EOSTrackingCollector(Algorithm[EmptyConfig]):
            def __init__(self, name: str, config: EmptyConfig) -> None:
                super().__init__(name, config)
                self.results: list[dict[str, Any]] = []
                self.eos_received = False

            async def on_received_data(self, message: Message[Any]) -> None:
                self.results.append(message.payload)

            async def on_received_ending(self, message: Message[Any]) -> None:
                self.eos_received = True

        orders = [
            {"order_id": "1", "product": "Widget"},
        ]
        customers = [
            {"order_id": "1", "name": "Alice"},
        ]

        yaml_content = create_join_pipeline_yaml(orders, customers)
        # Replace collector type with our EOS-tracking one
        yaml_content = yaml_content.replace(
            "type: result_collector", "type: eos_tracking_collector"
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None
            assert len(collector.results) == 1
            assert collector.eos_received is True, (
                "Joiner did not propagate EOS downstream"
            )
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestNestedKeyExtraction:
    """Tests for nested key extraction in joins."""

    @pytest.mark.asyncio
    async def test_nested_dot_notation_key(self, register_join_components):
        """Test join with nested dot notation correlation key."""

        @joiner("nested_key_enricher")
        class NestedKeyEnricher(Joiner[EmptyConfig]):
            async def on_joined(
                self,
                key: Any,
                messages: dict[str, list[Message[Any]]],
            ) -> None:
                order = messages.get("order_source", [None])[0]
                customer = messages.get("customer_source", [None])[0]

                if order and customer:
                    await self.send_data({
                        "key": key,
                        "order": order.payload,
                        "customer": customer.payload,
                    })

        @data_provider("nested_order_provider")
        class NestedOrderProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                orders = [
                    {"data": {"order_id": "1"}, "product": "Widget"},
                    {"data": {"order_id": "2"}, "product": "Gadget"},
                ]
                for order in orders:
                    await self.send_data(order)
                await self.send_end_of_stream()

        @data_provider("nested_customer_provider")
        class NestedCustomerProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                customers = [
                    {"data": {"order_id": "1"}, "name": "Alice"},
                    {"data": {"order_id": "2"}, "name": "Bob"},
                ]
                for customer in customers:
                    await self.send_data(customer)
                await self.send_end_of_stream()

        yaml_content = """
global:
  name: nested-key-test
  version: "1.0"

data_providers:
  - name: order_source
    type: nested_order_provider
  - name: customer_source
    type: nested_customer_provider

joiners:
  - name: enricher
    type: nested_key_enricher
    join:
      correlation_key_path: data.order_id
      mode: inner
      sources: [order_source, customer_source]

algorithms:
  - name: collector
    type: result_collector

connections:
  - source: order_source
    targets: [enricher]
  - source: customer_source
    targets: [enricher]
  - source: enricher
    targets: [collector]
"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            engine = Engine(config_path)
            await engine.run()

            collector = engine.components.get("collector")
            assert collector is not None
            assert len(collector.results) == 2

            # Verify the nested keys were extracted correctly
            keys = {r["key"] for r in collector.results}
            assert keys == {"1", "2"}
        finally:
            Path(config_path).unlink(missing_ok=True)
