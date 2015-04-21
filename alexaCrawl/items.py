from scrapy.item import Item, Field

class Page(Item):
    url = Field()
    title = Field()
    body = Field()
    content_length=Field()
    response_header=Field()
    response_connection=Field()
    size=Field()
    #content_type=Field()
    referer=Field()
    newcookies=Field()
    httpResponseStatus=Field()
    response_meta=Field()
    
    #description = Field()
    #category = Field()
    #url = Field()
    #description = Field()
    #category = Field()
