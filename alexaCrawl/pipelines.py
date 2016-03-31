# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


# class AlexacrawlPipeline(object):
#     def process_item(self, item, spider):
#         if some_flag:
#             spider.close_down = True


class AlexacrawlPipeline(object):
    def process_item(self, item, spider):
        return item


# from scrapy import log
# from datetime import datetime
# from scrapy.crawler import Crawler
# from scrapy.xlib.pydispatch import dispatcher
# from scrapy import signals
# import csv
# import re

# class AlexacrawlPipeline(object):

#     def __init__(self):
#         dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
#         dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

#     def spider_opened(self, spider):
#         log.msg("opened spider  %s at time %s" % (spider.name,datetime.now().strftime('%H-%M-%S')))
#         self.exampleCsv = csv.writer(open("%s.csv"% (spider.name), "wb"),
#                    delimiter=',', quoting=csv.QUOTE_MINIMAL)
#         self.exampleCsv.writerow(['index','depth_level','httpResponseStatus','content_length','url',
#         						  'newcookies','tagType','CNAMEChain','destIP','ASN_Number','ImgCount',
#         						  'ScriptCount','LinkCount','EmbededCount','start_time','end_time'])           

#     def process_item(self, item, spider):
#         #log.msg("Processsing item " + item['title'], level=log.DEBUG)
#         self.exampleCsv.writerow([item['index'],
#                                     item['depth_level'],
#                                     item['httpResponseStatus'],
#                                     item['content_length'].encode('utf-8'),
#                                     item['url'].encode('utf-8'),
#                                     item['newcookies'],
#                                     item['tagType'].encode('utf-8'),
#                                     item['CNAMEChain'],
#                                     item['destIP'],
#                                     item['ASN_Number'],
#                                     item['start_time'],
#                                     item['end_time']])
#         return item


#     def spider_closed(self, spider):
#         log.msg("closed spider %s at %s" % (spider.name,datetime.now().strftime('%H-%M-%S')))

    #def process_item(self, item, spider):
     #   return item
    