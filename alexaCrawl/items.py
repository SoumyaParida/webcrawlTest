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
    distinctASNs=Field()
    InternalObjectCount=Field()
    ExternalObjectCount=Field()
    NumberOfuniqueExternalSecondlevelSites=Field()
    start_time=Field()
    end_time=Field()
