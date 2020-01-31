import scrapy
import locale
import logging
import requests
import xml.etree.ElementTree as ET
from retry import retry
from urllib.parse import urljoin
from datetime import datetime
from sellers.items import SellerItem
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

SITEMAP_URL = "https://www.etsy.com/dynamic-sitemaps.xml?sitemap=shop_index_2"
SHOP_BASE_URL = "https://www.etsy.com/shop/"
NAMESPACE = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

def parse_xml(xml_string):
    return ET.fromstring(xml_string)

def strip_seller_name(shop_url):
    return str(shop_url).strip(SHOP_BASE_URL)

@retry(Exception, tries=1, delay=5, backoff=2)
def get_sitemap():
    return requests.get(SITEMAP_URL).content

def get_sellers_info_urls(parsed_sitemap):
    sellers_info_urls = []
    for entry in parsed_sitemap.findall(f"{NAMESPACE}sitemap"):
        sellers_info_urls.append(entry.find(f"{NAMESPACE}loc").text)
    return sellers_info_urls

class SellersSpider(scrapy.Spider):
    name = "sellers"

    def start_requests(self):
        raw_sitemap = get_sitemap()
        parsed_sitemap = parse_xml(raw_sitemap)
        urls = get_sellers_info_urls(parsed_sitemap)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # print(f"Crawling {response.url}")
        urls = response.xpath("/*[name()='urlset']/*[name()='url']/*[name()='loc']/text()").getall()
        names = list(map(strip_seller_name, urls))
        # print(f"Got {len(names)} stores!")
        lastmods = response.xpath("/*[name()='urlset']/*[name()='url']/*[name()='lastmod']/text()").getall()
        return [
            SellerItem(name=name, url=url, lastmod=lastmod) for
            name, url, lastmod in zip(names, urls, lastmods)
        ]
