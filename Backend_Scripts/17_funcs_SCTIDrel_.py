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

def DeleteAllNodesRels(tx, nodeTypes, queryLocation):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)
        with open(queryLocation, 'a') as file:
            file.write(f'''//DeleteAllNodesRels''')
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





def link_Concepts(tx, s0, s0_code, s1, s1_code, queryLocation):
    qLinkSCTs1 = f''' 
            MATCH ( n1:Concept )
            MATCH ( n2:Concept ) 
            where n1.conceptId = {s0} AND n1.conceptCode = {s0_code} AND n2.conceptId = {s1}  AND n2.conceptCode = {s1_code}
            CREATE ( n1 ) -[:ANCESTOR_OF]-> ( n2 );
            '''



    print(qLinkSCTs1)
    tx.run(qLinkSCTs1)
    with open(queryLocation, 'a') as file:
        file.write(f'''//link_Concepts''')
        file.write(f'''\n{qLinkSCTs1}\n\n''')


def modify_concept(tx, queryLocation):
    qLinkSCTs1 = f''' 
            MATCH (n:Concept) 
            SET
                n.Category= "Absolute" ,
                n.ID= toString(n.conceptId)
                '''

    print(qLinkSCTs1)
    Result = tx.run(qLinkSCTs1).consume().counters
    myNeo.Neo4J_properties_set(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//modify_concept''')
        file.write(f'''\n{qLinkSCTs1}\n\n''')







################################################################################

