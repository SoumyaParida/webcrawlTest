from scrapy.item import Item, Field

class alexaSiteInfoItem(Item):
    # define the fields for your item here like:
    name = Field()
    url = Field()
    description = Field()
    category = Field()

class alexaCategoryItem(Item):
    name = Field()
    url = Field()