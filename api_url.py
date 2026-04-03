import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from db_config import load_pending_urls, initialize_details_table, save_detail_records,update_url_status

QUEUE_TABLE_NAME = "product_url_queue"

MAX_THREAD_WORKERS = 5
RECORDS_PER_BATCH = 20

local_thread_storage = local()

request_headers = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest"
}


def get_thread_session():
    if not hasattr(local_thread_storage, "session"):
        session = requests.Session()
        session.headers.update(request_headers)
        local_thread_storage.session = session
    return local_thread_storage.session


def split_into_chunks(dataset, chunk_size):
    for i in range(0, len(dataset), chunk_size):
        yield dataset[i:i + chunk_size]


def remove_empty_fields(raw_dict):
    return {
        key: val for key, val in raw_dict.items()
        if val not in [None, "", [], {}]
    }


def extract_product_info(api_response, prod_id, cat_id):
    product_data = api_response.get("product", {})

    product_record = {}

    product_record["product_id"] = prod_id
    product_record["category_id"] = cat_id
    product_record["product_name"] = product_data.get("shortDescription")
    product_record["product_sku"] = product_data.get("sku")
    product_record["list_price"] = product_data.get("basicListPrice")
    product_record["image_url"] = product_data.get("smallImagePath")
    product_record["weight_info"] = product_data.get("shippingWeight")
    product_record["full_description"] = product_data.get("htmlContent")
    product_record["availability_status"] = product_data.get("availability", {}).get("message")
    product_record["product_attributes"] = []

    attribute_list = product_data.get("attributeTypes", [])

    for attribute_entry in attribute_list:
        attr_label = attribute_entry.get("label")
        attr_values = attribute_entry.get("attributeValues", [])

        for attr_val in attr_values:
            display_value = attr_val.get("valueDisplay")

            if attr_label and display_value:
                product_record["product_attributes"].append({
                    "key": attr_label,
                    "value": display_value
                })

    product_record = remove_empty_fields(product_record)
    return product_record


def fetch_single_product(page_url):
    session = get_thread_session()

    catalog_api = f"https://www.tacomascrew.com/api/v1/catalogpages?path={page_url}"
    print("Catalog API :", catalog_api)

    try:
        catalog_response = session.get(catalog_api, timeout=30)
        catalog_response.raise_for_status()
        catalog_data = catalog_response.json()
    except Exception as e:
        print(f"Error fetching catalog data for {page_url}: {e}")
        update_url_status(page_url, "failed")
        return None

    cat_id = catalog_data.get("category", {}).get("id")
    prod_id = catalog_data.get("productId")

    if not cat_id or not prod_id:
        print(f"Missing Category or Product ID for URL: {page_url}")
        update_url_status(page_url, "failed")
        return None

    detail_api = (
        f"https://www.tacomascrew.com/api/v1/products/{prod_id}"
        f"?addToRecentlyViewed=true"
        f"&applyPersonalization=true"
        f"&categoryId={cat_id}"
        f"&expand=documents,specifications,styledproducts,htmlcontent,attributes,crosssells,pricing,relatedproducts,brand"
        f"&getLastPurchase=true"
        f"&includeAlternateInventory=true"
        f"&includeAttributes=IncludeOnProduct,NotFromCategory"
        f"&replaceProducts=false"
    )

    print("Detail API :", detail_api)

    try:
        detail_response = session.get(detail_api, timeout=30)
        detail_response.raise_for_status()
        detail_data = detail_response.json()
    except Exception as e:
        print(f"Error fetching detail data for product {prod_id}: {e}")
        update_url_status(page_url, "failed")
        return None

    product_record = extract_product_info(detail_data, prod_id, cat_id)

    if product_record:
        print(f"Successfully processed Product ID: {product_record.get('product_id')}")
        print(f"Product Name: {product_record.get('product_name')}")
        update_url_status(page_url, "completed")
        return product_record
    else:
        update_url_status(page_url, "failed")
        return None


def handle_url_batch(url_batch):
    collected_products = []

    with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
        future_tasks = [executor.submit(fetch_single_product, url) for url in url_batch]

        for completed_task in as_completed(future_tasks):
            outcome = completed_task.result()
            if outcome:
                collected_products.append(outcome)

    if collected_products:
        save_detail_records(collected_products)


def run_url_fetcher():
    all_pending_urls = load_pending_urls(QUEUE_TABLE_NAME)
    print("Total URLs :", len(all_pending_urls))

    url_batches = list(split_into_chunks(all_pending_urls, RECORDS_PER_BATCH))
    print("Total Batches :", len(url_batches))

    for batch_index, url_batch in enumerate(url_batches, start=1):
        print(f"\nProcessing Batch {batch_index} | URLs : {len(url_batch)}")
        handle_url_batch(url_batch)


def main():
    initialize_details_table()
    run_url_fetcher()


if __name__ == "__main__":
    main()