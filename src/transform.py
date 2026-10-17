def transform_product(product):
    return {
        "product_id": product["id"],
        "title": product["title"],
        "category": product["category"],
        "brand": product.get("brand") or "Unknown",
        "sku": product["sku"],
        "price": product["price"],
        "discount_percentage": product["discountPercentage"],
        "rating": product["rating"],
        "stock": product["stock"],
        "availability_status": product["availabilityStatus"],
        "minimum_order_quantity": product["minimumOrderQuantity"],
        "created_at": product["meta"]["createdAt"],
        "updated_at": product["meta"]["updatedAt"]
    }

def transform_products(products):
    transformed_products = []

    for product in products:
        transformed_product = transform_product(product)
        transformed_products.append(transformed_product)

    return transformed_products