import json
from pygraphml import Graph
from neo4jrestclient.client import GraphDatabase
from pygraphml import GraphMLParser

gdb = GraphDatabase("http://ec2-52-23-203-124.compute-1.amazonaws.com:7474/db/data", username="neo4j", password="neo4j2")

with open("manhattan_cluster_labels.json") as data_file:    
    data = json.load(data_file)
    

sortedData = sorted(data.items(), key=lambda data: len(data[1]), reverse=True)

Outg = Graph()


for i in range(1,100):
    print "Processing records : "  + str(i) + " Row count: " + str(len(sortedData[i][1]))
    graphProps = {}
    graphProps["NodeCount"] = len(sortedData[i][1])
    graphProps["PhysicianCount"] = 0
    graphProps["ClinicCount"] = 0
    graphProps["PhysicianNames"] = ""
    graphProps["ClinicNames"] = ""
    graphProps["countMale"] = 0
    graphProps["countFemale"] = 0
    for neoId in sortedData[i][1]:
        print "Processing Id : " + str(neoId)
        props = gdb.node[neoId].properties
        if props.has_key("firstName"):
            graphProps["PhysicianCount"] += 1
            graphProps["PhysicianNames"] += props["firstName"] + " " + props["lastName"] + ". "
            if props["gender"] == "M":
                graphProps["countMale"] += 1
            if props["gender"] == "F":
                graphProps["countFemale"] += 1
        else:
            graphProps["ClinicCount"] += 1
            graphProps["ClinicNames"] += props["businessName"] + ". "
    node = Outg.add_node(i)
    node['NodeCount'] = graphProps["NodeCount"]
    node['PhysicianCount'] = graphProps["PhysicianCount"]
    node['ClinicCount'] = graphProps["ClinicCount"]
    node['PhysicianNames'] = graphProps["PhysicianNames"]
    node['ClinicNames'] = graphProps["ClinicNames"]
    node['countMale'] = graphProps["countMale"]
    node['countFemale'] = graphProps["countFemale"]

        

parser = GraphMLParser()
parser.write(Outg, "myGraph.graphml")
