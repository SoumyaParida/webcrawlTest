# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class AlexacrawlPipeline(object):
	def process_item(self, item, spider):
    if some_flag:
        spider.close_down = True

    #def process_item(self, item, spider):
     #   return item
    