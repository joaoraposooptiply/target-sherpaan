# target-sherpaan

A [Singer](https://www.singer.io/) target for sending PurchaseOrders to the Sherpa API.

## Overview

This target sends Purchase Orders to the Sherpa API using SOAP 1.2 protocol. It performs a two-step process:

1. **AddOrderedPurchase**: Creates a new purchase order
2. **ChangePurchase2**: Adds purchase order lines to the created purchase order

## Installation

```bash
pip install target-sherpaan
```

Or install from source:

```bash
git clone <repository-url>
cd target-sherpaan
pip install -e .
```

## Configuration

The target requires the following configuration:

- `shop_id` (required): The shop ID for the Sherpa SOAP service
- `security_code` (required): Security code for authentication
- `base_url` (optional): Base URL for the Sherpa service (defaults to test environment: `https://sherpaservices-tst.sherpacloud.eu`)
- `timeout` (optional): Request timeout in seconds (default: 300)

### Example Configuration

```json
{
  "shop_id": "322",
  "security_code": "your-security-code-here",
  "base_url": "https://sherpaservices-tst.sherpacloud.eu",
  "timeout": 300
}
```

## Usage

### Input Schema

The target expects records with the following schema:

```json
{
  "supplier_remoteId": "GAM002",
  "id": "optest123456_4d",
  "warehouse_code": "MAINWAREHOUSE",
  "transaction_date": "2025-11-14T08:24:34.000000Z",
  "created_at": "2025-11-28T00:00:00.000000Z",
  "line_items": [
    {
      "product_remoteId": "ITM007",
      "supplier_item_code": "ITM007",
      "quantity": 5
    },
    {
      "product_remoteId": "ITM013",
      "supplier_item_code": "ITM013",
      "quantity": 10
    }
  ],
  "externalid": "optest123456_4d"
}
```

**Note**: The `created_at` field from the order level is used as the `expected_date` for all purchase order lines.

### Running the Target

```bash
target-sherpaan --config config.json < input.jsonl
```

Or with Meltano:

```yaml
loaders:
  - name: target-sherpaan
    pip_url: target-sherpaan
    config:
      shop_id: "322"
      security_code: "your-security-code"
      base_url: "https://sherpaservices-tst.sherpacloud.eu"
```

## How It Works

1. **Step 1 - Create Purchase Order**: The target sends a `AddOrderedPurchase` SOAP request with:
   - `securityCode`: Authentication code
   - `supplierCode`: Supplier code from the record
   - `reference`: Reference for the purchase order
   - `warehouseCode`: Warehouse code

2. **Step 2 - Add Purchase Lines**: After receiving the purchase order number from step 1, the target sends a `ChangePurchase2` SOAP request with:
   - `securityCode`: Authentication code
   - `purchaseOrderNumber`: The purchase order number from step 1
   - `purchaseLines`: Array of purchase line items with:
     - `ItemCode`: Product remote ID from `product_remoteId`
     - `SupplierItemCode`: Supplier item code (defaults to ItemCode if not provided)
     - `QuantityOrdered`: Quantity from `quantity` field
     - `ExpectedDate`: Expected delivery date from order-level `created_at` field (applied to all lines)

## Development

### Setup

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### Testing

```bash
pytest
```

## License

Apache 2.0

