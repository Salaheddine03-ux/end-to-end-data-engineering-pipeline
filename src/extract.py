import requests
import logging

logger = logging.getLogger("etl_pipeline")
URL = "https://dummyjson.com/products"


def extract_products():
    all_products = []
    limit = 30
    skip = 0

    while True:
        response = requests.get(
            URL,
            params={"limit": limit, "skip": skip},
            timeout=10
        )

        response.raise_for_status()
        data = response.json()

        products = data["products"]
        total = data["total"]

        all_products.extend(products)

        logger.info(
            "Extraction progress | %s/%s products",
            len(all_products),
                total
                    )

        if len(all_products) >= total or not products:
            break

        skip += len(products)

    return all_products


if __name__ == "__main__":
    products = extract_products()
    print(f"\nTotal extracted: {len(products)}")