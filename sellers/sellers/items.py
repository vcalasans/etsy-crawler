import scrapy

class SellerItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    lastmod = scrapy.Field(serializer=str)
    lastUpdated = scrapy.Field(serializer=str)
    lastUpdateData = scrapy.Field(serializer=str)
