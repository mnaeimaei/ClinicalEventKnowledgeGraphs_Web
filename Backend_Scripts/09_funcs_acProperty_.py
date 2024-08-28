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

def clearConstraint(tx, x, driver,NodeEntity, queryLocation):
    for item in NodeEntity:
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





def createProperty(tx, acID, activityName, activitySynonym, label, Value, queryLocation):

    qCreateEntity = f'''           
            MERGE (d:Feature {{
            ID:"{acID}",          
            Name:"{activityName}",          
            Synonym:"{activitySynonym}",
            label:"{label}",          
            Value:"{Value}"
            }});
            '''



    qTest = f'''
            ######### Testing:#######################################
            MATCH (d:Feature)
            WHERE n.ID = "{acID}" 
            return n;
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    myNeo.Neo4J_label_node_property(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createProperty''')
        file.write(f'''\n{qCreateEntity}\n\n''')




def createEnProperty(tx, pID, queryLocation):

    qCreateEntity = f'''
            MATCH (e)  WHERE "{pID}" in  e.Activity_Properties_ID
            MATCH (d:Feature)  WHERE d.ID = "{pID}"
            CREATE (e)-[:Assign]->(d);
            '''

    qTest = f'''
            ######### Testing:#######################################
            return (e)-[:Assign]->(d);
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    myNeo.Neo4J_relationship_create(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createEnProperty''')
        file.write(f'''\n{qCreateEntity}\n\n''')


