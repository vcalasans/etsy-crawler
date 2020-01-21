import scrapy
import locale
import logging
from urllib.parse import urljoin
from datetime import datetime
from sellers_data.items import SellersDataItem
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

logging.getLogger('elasticsearch').setLevel(logging.WARNING)

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

class SellersDataSpider(scrapy.Spider):
    name = "sellers_data"

    def start_requests(self):
        client = Elasticsearch()
        search = Search(using=client, index='sellers') \
            .query('match_all') \
            .source(fields=['url']) \
            .sort({"lastmod": "desc"}) \
            .sort({"lastUpdatedData": {"order": "desc", "missing": "_first"}})[0:1000]
        response = search.execute()
        urls = [hit.url for hit in response]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        number_of_sales = response.xpath(f'//span[contains(@class, "shop-sales")]/descendant-or-self::*/text()').re_first(r'(\d+) Sales')
        if number_of_sales:
            number_of_sales = int(number_of_sales)
        number_of_reviews = response.xpath('//span[contains(@class, "rating-count")]/text()').re_first(r'\((.*)\)')
        if number_of_reviews:
            number_of_reviews = int(number_of_reviews)
        on_etsy_since = response.xpath('//span/text()').re_first(r'On Etsy since (.*)')
        if on_etsy_since:
            on_etsy_since = int(on_etsy_since)
        number_of_listings = response.xpath('//script/text()').re_first(r'"listings_total_count":(\d+)')
        if number_of_listings:
            number_of_listings = int(number_of_listings)
        number_of_listings_in_page = len(response.xpath('//span[@class="currency-value"]').getall())
        if number_of_listings_in_page == 0:
            free_shipping_percent = 0
        else:
            free_shipping_percent = len(response.xpath('//span/text()').re(r'FREE shipping')) / number_of_listings_in_page
        if free_shipping_percent:
            free_shipping_percent = float(free_shipping_percent)
        prices = list(map(locale.atof, response.xpath('//span[@class="currency-value"]/text()').getall()))
        if len(prices) == 0:
            avg_price = 0
        else:
            avg_price = sum(prices) / len(prices)
        return SellersDataItem(
            name=response.url.strip('https://www.etsy.com/shop/'),
            date=datetime.now(),
            number_of_sales=number_of_sales,
            number_of_reviews=number_of_reviews,
            number_of_listings=number_of_listings,
            on_etsy_since=on_etsy_since,
            free_shipping_percent=free_shipping_percent,
            avg_price=avg_price
        )
