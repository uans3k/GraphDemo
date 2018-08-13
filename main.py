from scrapy import cmdline
import logging
from py2neo import *

cmdline.execute("scrapy crawl CninfoSpider -s JOBDIR=crawls/job-release-0".split())
#  -s JOBDIR=crawls/job-0
# print("Start connect neo4j!")
# graph = Graph(
#     "bolt://127.0.0.1:7687"
#     , username="neo4j"
#     , password="123456"
#     )
# print("Connect Success!")
# trans = graph.begin()
# left = Node("God", godName="God2", prop="123", prop2="321")
# graph.merge(left, "God", {"godName","prop"})
# graph.nodes.match()
# trans.commit()
# left = Node("Company", name="uuu")
# trans = graph.begin()
#
#
# left=trans.graph.nodes.match("God", name="God2").first()
# # graph.match()
# right=trans.graph.nodes.match("Category",name="Domain").first()
# #
# left2Right = Relationship(left, "GodHasCategory", right,number="123321",)
# trans.create(left2Right)
# right = Node("Company"
#              , name="bbb"
#              , fullName="ccc"
#              )
#
# trans.merge(right,"Company","name")
# print(right)

# tmp = " 1.中国证券金融股份有限公司-wahaha-wakaka "
# tmp = tmp.strip()
# i=tmp.index(".")
# tmp=tmp[i+1:len(tmp)]
# print(tmp)
# tmpStrs = tmp.partition("-")
# print(tmpStrs)

# left = Node("Category", name="Domain")
# trans.merge(left, primary_label="Category", primary_key='name')
# right = Node("Company", name="uuu")
# trans.merge(right, primary_label="Company", primary_key='name')
# left=trans.pull(Node(lable="Category", name="Domain"))
# trans.push(Node(lable="Category", name="Domain"))
# right=trans.graph.nodes

# right = trans.graph.nodes.match("Company", name="bbb").first()
# right['fullName'] = "ccc"
# trans.push(right)
# trans.pull(right)
# print(left)
# print(right)
# left2Right = Relationship(left, "CategoryHasCompany", right)
# trans.merge(left2Right)
# trans.commit()
# print("Create Success!")
