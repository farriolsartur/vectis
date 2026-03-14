"""Join enrichment example components.

This module defines components for demonstrating stream joins:
- OrderProvider: Generates order data
- CustomerProvider: Generates customer data
- InventoryProvider: Generates inventory data
- OrderEnricher: Joins orders with customer and inventory data
- EnrichedOrderProcessor: Processes enriched orders
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from vectis import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
)
from vectis.components import Joiner, joiner


# =============================================================================
# Configuration Models
# =============================================================================


class OrderProviderConfig(BaseModel):
    """Configuration for order provider."""

    count: int = 10
    delay: float = 0.1


class CustomerProviderConfig(BaseModel):
    """Configuration for customer provider."""

    count: int = 10
    delay: float = 0.05


class InventoryProviderConfig(BaseModel):
    """Configuration for inventory provider."""

    count: int = 10
    delay: float = 0.08


class EnricherConfig(BaseModel):
    """Configuration for order enricher."""

    include_timestamps: bool = True


# =============================================================================
# Data Providers
# =============================================================================


@data_provider("order_api_provider")
class OrderProvider(DataProvider[OrderProviderConfig]):
    """Simulates an order API that streams order data."""

    async def run(self) -> None:
        for i in range(self.config.count):
            if self._stop_requested:
                break

            order = {
                "order_id": f"ORD-{i:04d}",
                "product_id": f"PROD-{i % 5:03d}",
                "quantity": (i % 10) + 1,
                "unit_price": 10.0 + (i % 50),
            }
            await self.send_data(order)

            if self.config.delay > 0:
                await asyncio.sleep(self.config.delay)

        await self.send_end_of_stream()


@data_provider("customer_api_provider")
class CustomerProvider(DataProvider[CustomerProviderConfig]):
    """Simulates a customer API that streams customer data."""

    CUSTOMER_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    async def run(self) -> None:
        for i in range(self.config.count):
            if self._stop_requested:
                break

            customer = {
                "order_id": f"ORD-{i:04d}",
                "customer_id": f"CUST-{i:04d}",
                "name": self.CUSTOMER_NAMES[i % len(self.CUSTOMER_NAMES)],
                "email": f"customer{i}@example.com",
                "tier": "gold" if i % 3 == 0 else "standard",
            }
            await self.send_data(customer)

            if self.config.delay > 0:
                await asyncio.sleep(self.config.delay)

        await self.send_end_of_stream()


@data_provider("inventory_api_provider")
class InventoryProvider(DataProvider[InventoryProviderConfig]):
    """Simulates an inventory API that streams inventory data."""

    async def run(self) -> None:
        for i in range(self.config.count):
            if self._stop_requested:
                break

            inventory = {
                "order_id": f"ORD-{i:04d}",
                "product_id": f"PROD-{i % 5:03d}",
                "stock_available": 100 - (i * 5),
                "warehouse": f"WH-{i % 3}",
            }
            await self.send_data(inventory)

            if self.config.delay > 0:
                await asyncio.sleep(self.config.delay)

        await self.send_end_of_stream()


# =============================================================================
# Joiner Component
# =============================================================================


@joiner("order_enricher")
class OrderEnricher(Joiner[EnricherConfig]):
    """Joins order data with customer and inventory information.

    This joiner correlates messages from three sources by order_id:
    - order_source: Order details
    - customer_source: Customer information
    - inventory_source: Inventory status

    When all three sources have data for an order, it emits an enriched
    order document combining all the information.
    """

    def __init__(self, name: str, config: EnricherConfig, join_config: Any) -> None:
        super().__init__(name, config, join_config)
        self.joined_count = 0
        self.partial_count = 0

    async def on_joined(
        self,
        key: Any,
        messages: dict[str, list[Message[Any]]],
    ) -> None:
        """Process a completed join and emit enriched order."""
        order_msg = messages.get("order_source", [None])[0]
        customer_msg = messages.get("customer_source", [None])[0]
        inventory_msg = messages.get("inventory_source", [None])[0]

        if not all([order_msg, customer_msg, inventory_msg]):
            # Shouldn't happen in INNER mode, but handle gracefully
            return

        order = order_msg.payload
        customer = customer_msg.payload
        inventory = inventory_msg.payload

        # Build enriched order
        enriched = {
            "order_id": key,
            "order": {
                "product_id": order["product_id"],
                "quantity": order["quantity"],
                "unit_price": order["unit_price"],
                "total": order["quantity"] * order["unit_price"],
            },
            "customer": {
                "id": customer["customer_id"],
                "name": customer["name"],
                "email": customer["email"],
                "tier": customer["tier"],
            },
            "inventory": {
                "stock_available": inventory["stock_available"],
                "warehouse": inventory["warehouse"],
                "can_fulfill": inventory["stock_available"] >= order["quantity"],
            },
        }

        if self.config.include_timestamps:
            enriched["timestamps"] = {
                "order_received": order_msg.timestamp.isoformat(),
                "customer_fetched": customer_msg.timestamp.isoformat(),
                "inventory_checked": inventory_msg.timestamp.isoformat(),
            }

        self.joined_count += 1
        await self.send_data(enriched)

    async def on_partial_join(
        self,
        key: Any,
        messages: dict[str, list[Message[Any]]],
        reason: str,
    ) -> None:
        """Handle incomplete joins."""
        self.partial_count += 1
        sources = list(messages.keys())
        print(f"  [PARTIAL] order_id={key}, sources={sources}, reason={reason}")


# =============================================================================
# Processor Algorithm
# =============================================================================


@algorithm("order_processor")
class EnrichedOrderProcessor(Algorithm[EmptyConfig]):
    """Processes enriched orders.

    This algorithm receives enriched orders from the joiner and
    processes them (in this example, just prints them).
    """

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.processed_count = 0
        self.fulfillable_count = 0

    async def on_received_data(self, message: Message[Any]) -> None:
        """Process an enriched order."""
        enriched = message.payload

        self.processed_count += 1
        if enriched.get("inventory", {}).get("can_fulfill", False):
            self.fulfillable_count += 1

        # Print order summary
        order = enriched.get("order", {})
        customer = enriched.get("customer", {})
        inventory = enriched.get("inventory", {})

        print(
            f"  [{self.processed_count}] "
            f"Order {enriched['order_id']}: "
            f"{customer.get('name', 'Unknown')} ordered "
            f"{order.get('quantity', '?')}x {order.get('product_id', '?')} "
            f"(${order.get('total', 0):.2f}) - "
            f"{'Fulfillable' if inventory.get('can_fulfill') else 'LOW STOCK'}"
        )

    async def on_received_ending(self, message: Message[Any]) -> None:
        """Print summary when stream ends."""
        print(
            f"\n  Summary: Processed {self.processed_count} orders, "
            f"{self.fulfillable_count} fulfillable"
        )
