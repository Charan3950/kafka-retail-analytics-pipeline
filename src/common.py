from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Literal
from uuid import UUID

from confluent_kafka.admin import AdminClient
from pydantic import BaseModel, ConfigDict, Field, field_validator


MONEY = Decimal("0.01")


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    order_id: str = Field(min_length=3, max_length=40)
    customer_id: str = Field(min_length=3, max_length=40)
    product_id: str = Field(min_length=2, max_length=40)
    product_name: str = Field(min_length=2, max_length=100)
    category: str = Field(min_length=2, max_length=60)
    region: Literal["North", "South", "East", "West"]
    quantity: int = Field(gt=0, le=100)
    unit_price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    discount_pct: Decimal = Field(ge=0, le=1, max_digits=5, decimal_places=4)
    status: Literal["created", "paid", "shipped", "cancelled"]
    event_time: datetime

    @field_validator("event_time")
    @classmethod
    def event_time_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must include a timezone")
        return value


class EnrichedOrder(OrderEvent):
    gross_amount: Decimal
    net_amount: Decimal
    processed_at: datetime


def transform_order(order: OrderEvent) -> EnrichedOrder:
    gross = (order.unit_price * order.quantity).quantize(MONEY, rounding=ROUND_HALF_UP)
    net = (gross * (Decimal("1") - order.discount_pct)).quantize(
        MONEY, rounding=ROUND_HALF_UP
    )
    return EnrichedOrder(
        **order.model_dump(),
        gross_amount=gross,
        net_amount=net,
        processed_at=datetime.now(timezone.utc),
    )


def json_bytes(value: BaseModel | dict) -> bytes:
    if isinstance(value, BaseModel):
        payload = value.model_dump(mode="json")
    else:
        payload = value
    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def wait_for_kafka(bootstrap_servers: str, attempts: int = 30) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    for attempt in range(1, attempts + 1):
        try:
            admin.list_topics(timeout=5)
            return
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(min(attempt, 5))
