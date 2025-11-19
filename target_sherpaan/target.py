"""Target Sherpaan class."""

from __future__ import annotations

from typing import List

from singer_sdk import Target
from singer_sdk import typing as th

from target_sherpaan import sinks


class TargetSherpaan(Target):
    """Target for Sherpaan PurchaseOrders."""

    name = "target-sherpaan"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "shop_id",
            th.StringType,
            required=True,
            description="The shop ID for the Sherpa SOAP service",
        ),
        th.Property(
            "security_code",
            th.StringType,
            required=True,
            secret=True,
            description="Security code for authentication",
        ),
        th.Property(
            "base_url",
            th.StringType,
            description="Base URL for the Sherpa service (defaults to test environment)",
            default="https://sherpaservices-prd.sherpacloud.eu",
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Request timeout in seconds",
            default=300,
        ),
    ).to_dict()

    def get_sinks(self) -> List[sinks.PurchaseOrderSink]:
        """Return a list of sinks."""
        return [
            sinks.PurchaseOrderSink(
                target=self,
                stream_name="purchase_orders",
            )
        ]


if __name__ == "__main__":
    TargetSherpaan.cli()

