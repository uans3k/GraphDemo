import logging

from py2neo import *
from scrapy.exceptions import DropItem
from Spider.items import *


class Neo4jPipeline(object):
    neo4j = None

    def __initNeo4j(self):
        self.neo4j.schema.create_uniqueness_constraint("Public", "name")
        self.neo4j.schema.create_uniqueness_constraint("Category", "name")
        self.neo4j.schema.create_uniqueness_constraint("Stock", "name")
        self.neo4j.schema.create_uniqueness_constraint("Subject", "name")
        self.neo4j.schema.create_uniqueness_constraint("Detail", "name")
        self.neo4j.schema.create_index("Senior", "name")

        self.neo4j.merge(Node('Public', name="中国大陆")
                         , "Public"
                         , "name"
                         )

        pass

    def open_spider(self, spider):
        logging.debug("Start connect neo4j!")
        self.neo4j = Graph("bolt://127.0.0.1:7687"
                           , username="neo4j"
                           , password="123456"
                           )
        logging.debug("Connect Success!")
        self.__initNeo4j()

    def process_item(self, item, spider):
        if isinstance(item, CategoryItem):
            self.__transcationRun(self.__processCategory(item))
        elif isinstance(item, StockItem):
            self.__transcationRun(self.__processStock(item))
        elif isinstance(item, SeniorItem):
            self.__transcationRun(self.__processSenior(item))
        elif isinstance(item, SubjectItem):
            self.__transcationRun(self.__processSubject(item))
        elif isinstance(item, ShareholderItem):
            self.__transcationRun(self.__processShareholder(item))
        else:
            raise DropItem("WTF ITEM")
        return item

    def __processCategory(self, item):
        def process(trans):
            nodeLeft = trans.graph.nodes.match("Public"
                                               , name="中国大陆"
                                               ).first()

            nodeRight = Node("Category"
                             , name=item['name']
                             )
            trans.merge(nodeRight, "Category", "name")

            left2Right = Relationship(nodeLeft, "PublicHasCategory", nodeRight)

            trans.create(left2Right)

        return process

    def __processStock(self, item):
        def process(trans):
            nodeLeft = trans.graph.nodes.match("Category"
                                               , name=item['category']
                                               ).first()
            nodeRight = Node("Stock"
                             , name=item['name']
                             , code=item['code']
                             )

            trans.merge(nodeRight, "Stock", "name")

            left2Right = Relationship(nodeLeft, "CategoryHasStock", nodeRight)
            trans.create(left2Right)
            # right2Left = Relationship(nodeRight, "CompanyBelong2Category", nodeLeft)
            # self.neo4j.create(right2Left)

        return process

    def __processSubject(self, item):
        def process(trans):
            nodeStock = Node("Stock"
                             , name=item['stock']
                             )
            trans.merge(nodeStock, "Stock", "name")

            nodeSubject = Node("Subject"
                               , name=item['name']
                               )
            trans.merge(nodeSubject, "Subject", "name")

            subject2Stock = Relationship(nodeSubject, "SubjectIssueStock", nodeStock)
            trans.create(subject2Stock)

            nodeDetail = Node("Detail"
                              , name=item['name']
                              , legalRepresentative=item['legalRepresentative']
                              , address=item['address']
                              , postCode=item['postCode']
                              , regCapital=item['regCapital']
                              , phoneNumber=item['phoneNumber']
                              , webSite=item['webSite']
                              , time2Market=item['time2Market']
                              )
            trans.merge(nodeDetail, "Detail", "name")

            subject2Detail = Relationship(nodeSubject, "SubjectHasDetail", nodeDetail)
            trans.create(subject2Detail)

        return process

    def __processSenior(self, item):
        def process(trans):
            nodeLeft = Node("Subject"
                            , name=item['subject']
                            )
            trans.merge(nodeLeft, "Subject", "name")

            nodeRight = trans.graph.nodes.match("Senior"
                                                , name=item['name']
                                                , bitrhday=item['birthday']
                                                , sex=item['sex']
                                                , education=item['education']
                                                ).first()
            if nodeRight is None:
                nodeRight = Node("Senior"
                                 , name=item['name']
                                 , bitrhday=item['birthday']
                                 , sex=item['sex']
                                 , education=item['education']
                                 )
                trans.create(nodeRight)
            else:
                pass

            left2Right = Relationship(nodeLeft, "SubjectHasSenior", nodeRight, job=item['job'])
            trans.create(left2Right)

        return process

    def __processShareholder(self, item):
        def process(trans):
            nodeObject = Node("Subject"
                              , name=item['subject']
                              )
            trans.merge(nodeObject, "Subject", "name")

            shareName = item['shareName']
            i = shareName.index(".")
            shareFullName = shareName[i + 1:len(shareName)]
            shareName = shareFullName.partition("-")[0]

            nodeSubject = Node("Subject"
                               , name=shareName
                               )
            trans.merge(nodeSubject, "Subject", "name")

            subject2object = Relationship(nodeSubject
                                          , "SubjectHoldStock"
                                          , nodeObject
                                          , shareName=shareFullName
                                          , shareQuantity=item['shareQuantity']
                                          , shareRatio=item['shareRatio']
                                          , shareNature=item['shareNature']
                                          )
            trans.create(subject2object)

        return process

    def __transcationRun(self, process):
        trans = self.neo4j.begin()
        process(trans)
        trans.commit()
