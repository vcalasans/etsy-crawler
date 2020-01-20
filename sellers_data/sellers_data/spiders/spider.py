import numpy as np
import scrapy
from urllib.parse import urljoin
from datetime import datetime
from sellers_data.items import SellersDataItem


class SellersDataSpider(scrapy.Spider):
    name = "sellers_data"

    def start_requests(self):
        urls = [
            'https://www.etsy.com/shop/7thmuse',
            'https://www.etsy.com/shop/debsattic2',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        print(f"URL={response.url}")

        sold_url = f"{response.url}/sold"
        sales_text = response.xpath(f'//a[@href="{sold_url}"]/text()').get()
        print(f'//a[@href="{sold_url}"]/text()')
        print(f"sales text {sales_text}")
        number_of_sales = int(str(sales_text).strip(' Sales'))
        number_of_reviews = int(response.xpath('//span[contains(@class, "rating-count")]/text()').re(r'\((.*)\)')[0])
        on_etsy_since = int(response.xpath('//span/text()').re(r'On Etsy since (.*)')[0])
        number_of_listings = int(response.xpath('//script/text()').re(r'"listings_total_count":(\d+)')[0])
        free_shipping_percent = len(response.xpath('//span/text()').re(r'FREE shipping')) / len(response.xpath('//span[@class="currency-value"]').getall())
        avg_price = np.average(map(float, response.xpath('//span[@class="currency-value"]/text()').getall()))
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
