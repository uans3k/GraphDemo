from scrapy import Item
from scrapy import Field


class CategoryItem(Item):
    name = Field()


class StockItem(Item):
    category = Field()
    name = Field()
    code = Field()


class SeniorItem(Item):
    subject = Field()
    name = Field()
    job = Field()
    birthday = Field()
    sex = Field()
    education = Field()
    pass


class SubjectItem(Item):
    stock = Field()
    name = Field()
    legalRepresentative = Field()
    address = Field()
    postCode = Field()
    regCapital = Field()
    phoneNumber = Field()
    webSite = Field()
    time2Market = Field()


class ShareholderItem(Item):
    subject = Field()
    shareName = Field()
    shareQuantity = Field()
    shareRatio = Field()
    shareNature = Field()
