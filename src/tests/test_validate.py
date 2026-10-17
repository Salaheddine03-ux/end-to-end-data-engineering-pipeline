from src.validate import validate_products


def test_valid_product():
    product = {
        "title": "Laptop",
        "price": 1200,
        "stock": 10,
        "rating": 4.5,
        "sku": "LAP-001"
    }

    valid, invalid = validate_products([product])

    assert len(valid) == 1
    assert len(invalid) == 0


def test_negative_price():
    product = {
        "title": "Laptop",
        "price": -100,
        "stock": 10,
        "rating": 4.5,
        "sku": "LAP-002"
    }

    valid, invalid = validate_products([product])

    assert len(valid) == 0
    assert len(invalid) == 1


def test_missing_title():
    product = {
        "title": "",
        "price": 1200,
        "stock": 10,
        "rating": 4.5,
        "sku": "LAP-002"
    }

    valid, invalid = validate_products([product])

    assert len(valid) == 0
    assert len(invalid) == 1


def test_invalid_rating():
    product = {
        "title": "Laptop",
        "price": 1200,
        "stock": 10,
        "rating": 7.5,
        "sku": "LAP-003"
    }

    valid, invalid = validate_products([product])

    assert len(valid) == 0
    assert len(invalid) == 1

def test_duplicate_sku():
    products = [
        {
            "title": "Laptop A",
            "price": 1200,
            "stock": 10,
            "rating": 4.5,
            "sku": "LAP-001"
        },
        {
            "title": "Laptop B",
            "price": 1500,
            "stock": 5,
            "rating": 4.8,
            "sku": "LAP-001"
        }
    ]

    valid, invalid = validate_products(products)

    assert len(valid) == 1
    assert len(invalid) == 1
