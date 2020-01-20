#!/usr/bin/env python
from progress.bar import Bar
import requests
from retry import retry
import xml.etree.ElementTree as ET

SITEMAP_URL = "https://www.etsy.com/dynamic-sitemaps.xml?sitemap=shop_index_2"
SHOP_BASE_URL = "https://www.etsy.com/shop/"
NAMESPACE = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

def strip_seller_name(shop_url):
    return str(shop_url).strip(SHOP_BASE_URL)

def parse_xml(xml_string):
    return ET.fromstring(xml_string)

@retry(Exception, tries=5, delay=5, backoff=2)
def get_sitemap():
    return requests.get(SITEMAP_URL).content

def get_sellers_info_urls(parsed_sitemap):
    sellers_info_urls = []
    for entry in sitemap.findall(f"{NAMESPACE}sitemap"):
        sellers_info_urls.append(entry.find(f"{NAMESPACE}loc").text)
    return sellers_info_urls

@retry(Exception, tries=5, delay=0.2, jitter=(0.1, 2), backoff=2)
def get_sellers_info(sellers_info_url):
    sellers_info = []
    raw_sellers_info_xml = requests.get(sellers_info_url).content
    sellers_info_xml = parse_xml(raw_sellers_info_xml)
    for entry in sellers_info_xml.findall(f"{NAMESPACE}url"):
        sellers_info.append(
            {
                "name": strip_seller_name(entry.find(f"{NAMESPACE}loc").text),
                "url": entry.find(f"{NAMESPACE}loc").text,
                "lastmod": entry.find(f"{NAMESPACE}lastmod").text,
            }
        )
    return sellers_info

@retry(Exception, tries=5, delay=0.5, backoff=2)
def save_sellers_info(sellers_info_list):
    print(sellers_info_list)
    print('saved!')


if __name__ == '__main__':
    raw_sitemap = get_sitemap()
    sitemap = parse_xml(raw_sitemap)
    sellers_info_urls = get_sellers_info_urls(sitemap)
    bar = Bar('Processing', max=len(sellers_info_urls))
    for url in sellers_info_urls:
        sellers_info = get_sellers_info(url)
        save_sellers_info(sellers_info)
        bar.next()
    bar.finish()