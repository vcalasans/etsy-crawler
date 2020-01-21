import logging

BOT_NAME = 'sellers_data'

SPIDER_MODULES = ['sellers_data.spiders']
NEWSPIDER_MODULE = 'sellers_data.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'sellers_data (+http://www.yourdomain.com)'

COOKIES_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   'Accept-Language': 'en',
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'sellers_data.pipelines.SellersDataPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False
TELNETCONSOLE_ENABLED = False

LOG_FILE = './sellers_data.log'
LOG_LEVEL = logging.INFO
