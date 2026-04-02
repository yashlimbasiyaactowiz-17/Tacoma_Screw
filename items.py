# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TacomaItem(scrapy.Item):
        main_category = scrapy.Field()
        main_url = scrapy.Field()
        subcategories = scrapy.Field()