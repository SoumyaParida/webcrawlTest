# -*- coding: utf-8 -*-

# Scrapy settings for alexaCrawl project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'alexaCrawl'

SPIDER_MODULES = ['alexaCrawl.spiders']
NEWSPIDER_MODULE = 'alexaCrawl.spiders'

#Instead of using multiple processes,we can start concurrent requests and assign number
#of items to those request.If required we will implement multiple processes using "Scrapyd"
#which has apis like "max_proc" and "max_proc_per_cpu" which will solve our issues.
CONCURRENT_REQUESTS ='50'
CONCURRENT_ITEMS ='200'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'alexaCrawl (+http://www.yourdomain.com)'
DOWNLOADER_MIDDLEWARES = {
   # 'misc.middleware.CustomHttpProxyMiddleware': 400,
    'misc.middleware.CustomUserAgentMiddleware': 401,
}

