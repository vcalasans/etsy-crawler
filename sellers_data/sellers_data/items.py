import scrapy

class SellersDataItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    date = scrapy.Field(serializer=str)
    number_of_sales = scrapy.Field()
    number_of_reviews = scrapy.Field()
    number_of_listings = scrapy.Field()
    on_etsy_since = scrapy.Field()
    free_shipping_percent = scrapy.Field()
    avg_price = scrapy.Field()
