def validate_products(products):
    valid_products = []
    invalid_products = []
    seen_skus = set()

    for product in products:
        errors = []

        if not product.get("title"):
            errors.append("Missing title")

        if product.get("price") is None or product["price"] < 0:
            errors.append("Invalid price")

        if product.get("stock") is None or product["stock"] < 0:
            errors.append("Invalid stock")

        rating = product.get("rating")
        if rating is None or not 0 <= rating <= 5:
            errors.append("Invalid rating")

        sku = product.get("sku")
        if not sku:
            errors.append("Missing SKU")
        elif sku in seen_skus:
            errors.append("Duplicate SKU")
        else:
            seen_skus.add(sku)

        if errors:
            invalid_products.append({
                "product": product,
                "errors": errors
            })
        else:
            valid_products.append(product)

    

    return valid_products, invalid_products