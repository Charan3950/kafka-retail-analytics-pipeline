from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from src.common import OrderEvent, json_bytes, transform_order


def valid_order(**overrides) -> OrderEvent:
    data = {
        "event_id": UUID("12345678-1234-5678-1234-567812345678"),
        "order_id": "ORD-123",
        "customer_id": "CUS-123",
        "product_id": "P100",
        "product_name": "Headphones",
        "category": "Electronics",
        "region": "South",
        "quantity": 2,
        "unit_price": Decimal("79.99"),
        "discount_pct": Decimal("0.10"),
        "status": "paid",
        "event_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    data.update(overrides)
    return OrderEvent(**data)


def test_transform_calculates_money_correctly() -> None:
    enriched = transform_order(valid_order())
    assert enriched.gross_amount == Decimal("159.98")
    assert enriched.net_amount == Decimal("143.98")
    assert enriched.processed_at.tzinfo is not None


def test_invalid_quantity_is_rejected() -> None:
    with pytest.raises(ValidationError):
        valid_order(quantity=0)


def test_unknown_region_is_rejected() -> None:
    with pytest.raises(ValidationError):
        valid_order(region="Central")


def test_naive_event_time_is_rejected() -> None:
    with pytest.raises(ValidationError):
        valid_order(event_time=datetime(2026, 1, 1))


def test_json_serializes_decimals_and_datetimes() -> None:
    output = json_bytes(transform_order(valid_order())).decode("utf-8")
    assert '"net_amount":"143.98"' in output
    assert '"event_time":"2026-01-01T00:00:00Z"' in output
