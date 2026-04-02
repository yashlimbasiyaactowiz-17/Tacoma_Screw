import scrapy
from scrapy.cmdline import execute


class TacomaspiderSpider(scrapy.Spider):
    name = "tacoma_spider"
    allowed_domains = ["tacomascrew.com"]
    start_urls = ["https://www.tacomascrew.com/all-categories"]

    headers = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0",
        "x-requested-with": "XMLHttpRequest"
    }

    def __init__(self, *args, **kwargs):
        super(TacomaspiderSpider, self).__init__(*args, **kwargs)
        self.visited = set()

    def parse(self, response):
        links = response.xpath("//div[@class='category-card x:px-lg x:mb-xxl']//a/@href").getall()

        for r in links:
            if r and r not in self.visited:
                self.visited.add(r)

                url = f"https://www.tacomascrew.com/api/v1/catalogpages?path={r}"

                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    callback=self.categories
                )

    def categories(self, response):
        data = response.json()
        main_cat = data.get("category", {})

        sub_cats = main_cat.get("subCategories")

        if sub_cats:
            for sub in sub_cats:
                path = sub.get("path")

                if path and path not in self.visited:
                    self.visited.add(path)

                    url = f"https://www.tacomascrew.com/api/v1/catalogpages?path={path}"

                    yield scrapy.Request(
                        url=url,
                        headers=self.headers,
                        callback=self.categories
                    )
        else:
            category_id = main_cat.get("id")

            if category_id:
                url = f"https://www.tacomascrew.com/api/v1/products?categoryId={category_id}&page=1"

                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    callback=self.parse_products,
                    meta={"category_id": category_id, "page": 1}
                )

    def parse_products(self, response):
        data = response.json()

        products = data.get("products", [])

        for prod in products:
            yield {
                "url": prod.get("productDetailUrl"),
                "name": prod.get("name")
            }

        # ✅ SAFE pagination
        pagination = data.get("pagination", {})

        current_page = pagination.get("page", 1)
        total_pages = pagination.get("totalPages", 1)

        if current_page < total_pages:
            next_page = current_page + 1
            category_id = response.meta.get("category_id")

            next_url = f"https://www.tacomascrew.com/api/v1/products?categoryId={category_id}&page={next_page}"

            yield scrapy.Request(
                url=next_url,
                headers=self.headers,
                callback=self.parse_products,
                meta={"category_id": category_id, "page": next_page}
            )


if __name__ == '__main__':
    execute("scrapy crawl tacoma_spider -o output.json".split())