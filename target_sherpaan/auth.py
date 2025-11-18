"""Authentication handling for target-sherpaan."""

from __future__ import annotations

from typing import Any, Dict


class SherpaAuth:
    """Authentication handler for Sherpa API."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize authentication with config.
        
        Args:
            config: Configuration dictionary containing shop_id and security_code
        """
        self.shop_id = config["shop_id"]
        self.security_code = config["security_code"]
        base_url = config.get(
            "base_url",
            "https://sherpaservices-prd.sherpacloud.eu"
        )
        # Ensure base_url doesn't have trailing slash and includes shop_id
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        self.base_url = f"{base_url}/{self.shop_id}/Sherpa.asmx"

