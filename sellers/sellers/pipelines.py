from pprint import pprint
from datetime import datetime
import logging
from elasticsearch_dsl import Document, Text, Date, Integer, Double
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch

logging.getLogger('elasticsearch').setLevel(logging.WARNING)

class Seller(Document):
    name = Text()
    url = Text()
    lastmod = Date()
    lastUpdated = Date()
    lastUpdatedData = Date()

    class Index:
        name = 'sellers'

class SellersPipeline:
    def open_spider(self, spider):
        connections.create_connection(hosts=['localhost'])
        self.client = Elasticsearch()
        Seller.init()

    def process_item(self, item, spider):
        data = Seller(meta={'id': item['name']})
        # print(f"updating {item['name']}")
        data.update(
            doc_as_upsert=True,
            name=item['name'],
            url=item['url'],
            lastmod=item['lastmod'],
            lastUpdated=datetime.now(),
        )

        return item
