from scrapy.item import Item, Field

class Page(Item):
    index=Field()
    depth_level=Field()
    httpResponseStatus=Field()
    content_length=Field()
    url = Field()
    newcookies=Field()
    tagType=Field()
    CNAMEChain=Field()
    destIP=Field()
    ASN_Number=Field()
    ImgCount=Field()
    ScriptCount=Field()
    LinkCount=Field()
    EmbededCount=Field()
    start_time=Field()
    end_time=Field()
    
    #depth_level=Field()
    #title = Field()
    #body = Field()
    #content_length=Field()
    #set_cache=Field()
    #response_header=Field()
    #response_connection=Field()
    #size=Field()
    #content_type=Field()
    #referer=Field()
    
    
    
    
    
    #description = Field()
    #category = Field()
    #url = Field()
    #description = Field()
    #category = Field()
