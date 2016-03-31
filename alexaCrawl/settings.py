# -*- coding: utf-8 -*-

# Scrapy settings for alexaCrawl project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
# import alexaCrawl.spiders.middleware
#from pybloomfilter import BloomFilter

BOT_NAME = 'alexaCrawl'

SPIDER_MODULES = ['alexaCrawl.spiders']
NEWSPIDER_MODULE = 'alexaCrawl.spiders'
#DUPEFILTER_CLASS = "alexaCrawl.spiders.alexawebcrawltest.BLOOMDupeFilter"
#CLOSESPIDER_PAGECOUNT = 1000
#CLOSESPIDER_TIMEOUT = 3600
ITEM_PIPELINES = {
    'alexaCrawl.pipelines.AlexacrawlPipeline': 800
}

DUPEFILTER_CLASS = 'scrapy.dupefilter.RFPDupeFilter'
DUPEFILTER_DEBUG = True

REDIRECT_ENABLED = True
RETRY_ENABLED = False
#Some websites blocked the BOT using cookies.
# REACTOR_THREADPOOL_MAXSIZE = 20
COOKIES_ENABLED = False
DEPTH_LIMIT = 1
WEBSERVICE_ENABLED=False
TELNETCONSOLE_ENABLED=False
# HTTPERROR_ALLOWED_CODES=[200,404,301,302]
HTTPERROR_ALLOW_ALL=True

# CONCURRENT_REQUESTS = 10
# CONCURRENT_REQUESTS_BY_DOMAIN=100

CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_BY_DOMAIN=50

DOWNLOAD_TIMEOUT = 15
AJAXCRAWL_ENABLED = True
# DOWNLOAD_MAXSIZE = 10741824
# DOWNLOAD_DELAY = 0.1 # 250 ms of delay

#settings.overrides['DOWNLOADER_MIDDLEWARES'] = {'seerspider.SpiderFailSignal': 901}

#RETRY_ENABLED=False
#DOWNLOAD_TIMEOUT=10
#DOWNLOAD_DELAY = 0
#REDIRECT_ENABLED = False
#COMPRESSION_ENABLED=False
#REDIRECT_ENABLED=False
# CONCURRENT_ITEMS = 100
# CONCURRENT_REQUESTS_PER_DOMAIN = 64
# #CONCURRENT_SPIDERS = 128
# DOWNLOAD_DELAY = 0
# DOWNLOADER_MIDDLEWARES = {
#     'alexaCrawl.spiders.middleware.ForceUTF8Response': 100,
# }

#Instead of using multiple processes,we can start concurrent requests and assign number
#of items to those request.If required we will implement multiple processes using "Scrapyd"
#which has apis like "max_proc" and "max_proc_per_cpu" which will solve our issues.

#CONCURRENT_REQUESTS ='50'
#CONCURRENT_ITEMS ='200'

#Some websites blocked the BOT using cookies.
#COOKIES_ENABLED =False


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'alexaCrawl (+http://www.yourdomain.com)'

