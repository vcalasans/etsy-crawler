import scrapy


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
        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)
