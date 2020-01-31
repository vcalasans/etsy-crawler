from datetime import datetime
import logging
from elasticsearch_dsl import Document, Text, Date, Integer, Double, Search
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch

logging.getLogger('elasticsearch').setLevel(logging.WARNING)

class SellerData(Document):
    name = Text()
    date = Date()
    numberOfSales = Integer()
    numberOfReviws = Integer()
    numberOfListings = Integer()
    onEtsySince = Integer()
    freeShippingPercent = Double()
    avgPrice = Double()

    class Index:
        name = 'sellers_data'

class SellersDataPipeline:
    def open_spider(self, spider):
        connections.create_connection(hosts=['localhost'])
        self.client = Elasticsearch()

    def process_item(self, item, spider):
        print(f"Crawling {item['name']}")
        data = SellerData(
            name=item['name'],
            date=item['date'],
            numberOfSales=item['number_of_sales'],
            numberOfReviews=item['number_of_reviews'],
            numberOfListings=item['number_of_listings'],
            onEtsySince=item['on_etsy_since'],
            freeShippingPercent=item['free_shipping_percent'],
            avgPrice=item['avg_price']
        )
        data.save()

        search = Search(using=self.client, index='sellers').query({'match': { 'name': item['name']}})
        id = search.execute()[0].meta.id
        self.client.update(index="sellers", id=id, body={"doc": {"lastUpdateData": datetime.now()} })

        return item
