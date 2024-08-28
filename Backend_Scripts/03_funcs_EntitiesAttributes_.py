import pandas as pd
import time, csv
from neo4j import GraphDatabase
import B02_neo4j as myNeo


def deleteRelation(tx, relationTypes, queryLocation):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r:{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)
        with open(queryLocation, 'a') as file:
            file.write(f'''//deleteRelation''')
            file.write(f'''\n{qDeleteRelation}\n\n''')


def DeleteNodes(tx, nodeTypes, queryLocation):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)
        with open(queryLocation, 'a') as file:
            file.write(f'''//DeleteNodes''')
            file.write(f'''\n{qDeleteNodes}\n\n''')

def deletePartiallyRel(tx, rel, where, val, queryLocation):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePartiallyRel''')
        file.write(f'''\n{qDeleteRelation}\n\n''')

def clearConstraint(tx, x, driver,nodeTypes, queryLocation):
    for item in nodeTypes:
        #print(item)
        for x in tx.run("SHOW CONSTRAINTS;"):
            if x is not None:
                if x["labelsOrTypes"] == [item]:
                    y = x["name"]
                    dropQ = "DROP CONSTRAINT " + y
                    print(dropQ," // for ",item)
                    Result = tx.run(dropQ).consume().counters
                    myNeo.Neo4J_removingConstraint(Result)
                    with open(queryLocation, 'a') as file:
                        file.write(f'''//clearConstraint''')
                        file.write(f'''\n{dropQ}\n\n''')
                else:
                    with open(queryLocation, 'a') as file:
                        file.write(f'''//clearConstraint''')
                        file.write(f'''\n//Database is empty and nothing to drop\n\n''')
            else:
                print("Database is empty and nothing to drop")
                with open(queryLocation, 'a') as file:
                    file.write(f'''//clearConstraint''')
                    file.write(f'''\n//Database is empty and nothing to drop\n\n''')

    qTest = f'''
            ######### Testing:#######################################
            SHOW CONSTRAINTS;
            ##############################################################
            '''
    print(qTest)

def createConstraint(tx, NodeEntity, queryLocation):

    for item in NodeEntity:
        qC = f'CREATE CONSTRAINT FOR (n:{item}) REQUIRE n.ID IS UNIQUE;'
        print(qC)
        Result = tx.run(qC).consume().counters
        myNeo.Neo4J_creatingConstraint(Result)
        with open(queryLocation, 'a') as file:
            file.write(f'''//createConstraint''')
            file.write(f'''\n{qC}\n\n''')

    qTest = f'''
            ######### Testing:#######################################
            :schema'
            ##############################################################
            '''

    print(qTest)




def OtherEntities_Nodes(tx, type, id , name, value, category, queryLocation):
    qCreateEntity = f'''
            CREATE (p:{type} {{Name : "{name}", ID: "{id}" , Value: "{value}" , Category: "Absolute" }});
            '''


    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (p:{type})
            return p;


            MATCH (p:{type})
            where p.ID="{id}"
            return p;
            ##############################################################
            '''

    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    myNeo.Neo4J_label_node_property(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//OtherEntities_Nodes''')
        file.write(f'''\n{qCreateEntity}\n\n''')


