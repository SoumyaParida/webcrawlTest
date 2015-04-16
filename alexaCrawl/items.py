from scrapy.item import Item, Field

class Page(Item):
    url = Field()
    title = Field()
    body = Field()
    size=Field()
    
    #description = Field()
    #category = Field()
    #url = Field()
    #description = Field()
    #category = Field()
