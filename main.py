import requests
import json
import os
import gzip
import time
from lxml import html
from db_config import initialize_url_table, save_url_records
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local

SITE_ROOT_URL = "https://www.tacomascrew.com/all-categories"

request_headers = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest"
}

MAX_THREAD_WORKERS = 3
RECORDS_PER_BATCH = 100

processed_category_urls = set()
category_url_lock = Lock()
local_thread_storage = local()

scraped_product_count = 0
product_count_lock = Lock()

failed_pagination_urls = []
failed_url_lock = Lock()


def get_thread_session():
    if not hasattr(local_thread_storage, "session"):
        session = requests.Session()
        session.headers.update(request_headers)
        local_thread_storage.session = session
    return local_thread_storage.session


def fetch_with_retry(target_url, retries=10, retry_delay=2):
    for attempt_no in range(retries):
        try:
            session = get_thread_session()
            response = session.get(target_url, timeout=(5, 30))
            response.raise_for_status()
            return response
        except Exception as error:
            print(f"Request Failed | Attempt {attempt_no + 1} | URL : {target_url} | Error : {error}")
            time.sleep(retry_delay)

    return None


def split_into_chunks(dataset, chunk_size):
    for i in range(0, len(dataset), chunk_size):
        yield dataset[i:i + chunk_size]


def crawl_root_categories():
    response = fetch_with_retry(SITE_ROOT_URL)
    if not response:
        print("Root Page Failed :", SITE_ROOT_URL)
        return

    page_tree = html.fromstring(response.text)
    category_cards = page_tree.xpath("//div[@class='category-card x:px-lg x:mb-xxl']")

    root_category_list = []

    for card in category_cards:
        category_path = card.xpath("string(.//a/@href)").strip()

        if category_path:
            catalog_api = f"https://www.tacomascrew.com/api/v1/catalogpages?path={category_path}"
            root_category_list.append((catalog_api, category_path))

    print("Total Root Categories :", len(root_category_list))

    for api_url, path in root_category_list:
        print("Root Category URL :", api_url)

    return crawl_category_level(root_category_list, level="root")


def process_category_page(catalog_api, category_path, level):
    root_save_dir = "root_categories"
    sub_save_dir = "sub_categories"

    os.makedirs(root_save_dir, exist_ok=True)
    os.makedirs(sub_save_dir, exist_ok=True)

    with category_url_lock:
        if catalog_api in processed_category_urls:
            return
        processed_category_urls.add(catalog_api)

    response = fetch_with_retry(catalog_api)
    if not response:
        print("Category Page Failed :", catalog_api)
        return

    category_data = response.json()

    safe_filename = category_path.replace("/", "_").strip("_")

    if level == "root":
        with gzip.open(f"{root_save_dir}\\{safe_filename}.json.gz", "wt", encoding="utf-8") as gz_file:
            json.dump(category_data, gz_file, indent=4)
    else:
        with gzip.open(f"{sub_save_dir}\\{safe_filename}.json.gz", "wt", encoding="utf-8") as gz_file:
            json.dump(category_data, gz_file, indent=4)

    child_categories = category_data.get("category", {}).get("subCategories", [])

    if child_categories:
        child_category_list = []

        for child_cat in child_categories:
            child_path = child_cat.get("path")

            if child_path:
                child_api = f"https://www.tacomascrew.com/api/v1/catalogpages?path={child_path}"
                child_category_list.append((child_api, child_path))

        if child_category_list:
            crawl_category_level(child_category_list, level="sub")
    else:
        category_id = category_data.get("category", {}).get("id")

        if category_id:
            product_listing_api = f"https://www.tacomascrew.com/api/v1/products?categoryId={category_id}"

            print("Product Listing API :", product_listing_api)
            paginate_and_collect_products(product_listing_api, category_path, category_id)


def crawl_category_level(category_url_list, level="sub"):
    with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
        future_tasks = [
            executor.submit(process_category_page, api_url, category_path, level)
            for api_url, category_path in category_url_list
        ]

        for completed_task in as_completed(future_tasks):
            completed_task.result()


def store_product_batch(product_batch, category_id):
    global scraped_product_count

    records_to_insert = []

    for product_item in product_batch:
        product_title = product_item.get("name")
        product_page_url = product_item.get("productDetailUrl")
        product_ref_id = product_item.get("id")

        print(f"Product Name: {product_title}")
        print(f"Product URL: {product_page_url}")
        print(f"Category ID: {category_id}")

        if product_title and product_page_url:
            # Now includes category_id as the 4th parameter
            records_to_insert.append((product_title, product_page_url, product_ref_id, category_id, "pending"))

    if records_to_insert:
        save_url_records(records_to_insert)

        with product_count_lock:
            scraped_product_count += len(records_to_insert)
            print(f"Total Products Collected: {scraped_product_count}")


def paginate_and_collect_products(product_listing_api, category_path, category_id):
    product_save_dir = "product_pages"

    os.makedirs(product_save_dir, exist_ok=True)

    safe_filename = category_path.replace("/", "_").strip("_")
    current_page_url = product_listing_api
    page_number = 1

    with gzip.open(f"{product_save_dir}\\{safe_filename}.json.gz", "wt", encoding="utf-8") as gz_file:
        gz_file.write("")

    while current_page_url:
        print(f"Fetching Page {page_number} : {current_page_url}")

        page_response = fetch_with_retry(current_page_url, retries=10, retry_delay=2)

        if not page_response:
            print("Page Fetch Failed :", current_page_url)

            with failed_url_lock:
                failed_pagination_urls.append(current_page_url)

            break

        page_data = json.loads(page_response.text)

        with gzip.open(f"{product_save_dir}\\{safe_filename}.json.gz", "at", encoding="utf-8") as gz_file:
            gz_file.write(json.dumps(page_data, indent=4))
            gz_file.write("\n")

        product_list = page_data.get("products", [])
        print(f"Page {page_number} | Products Found : {len(product_list)}")

        if product_list:
            page_batches = list(split_into_chunks(product_list, RECORDS_PER_BATCH))

            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                future_tasks = [
                    executor.submit(store_product_batch, batch, category_id)
                    for batch in page_batches
                ]

                for completed_task in as_completed(future_tasks):
                    completed_task.result()

        current_page_url = page_data.get("pagination", {}).get("nextPageUri")
        print("Next Page URL :", current_page_url)

        page_number += 1
        time.sleep(0.2)


def main():
    initialize_url_table()
    crawl_root_categories()

    print("\nTotal Products Collected :", scraped_product_count)

    if failed_pagination_urls:
        print("\nFailed Pages :")
        for failed_url in failed_pagination_urls:
            print(failed_url)

        with open("failed_pages.txt", "w", encoding="utf-8") as txt_file:
            for failed_url in failed_pagination_urls:
                txt_file.write(failed_url + "\n")

        print("\nFailed URLs Saved In : failed_pages.txt")
    else:
        print("\nNo Failed Pages")


if __name__ == "__main__":
    main()