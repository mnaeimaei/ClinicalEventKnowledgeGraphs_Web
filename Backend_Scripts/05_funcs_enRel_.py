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

def intraEntitiesRel(tx, Type1, ID1, Type2,ID2, queryLocation):

    qCreateEntity = f'''
            MATCH (n1:{Type1})  WHERE n1.ID = "{ID1}" 
            MATCH (n2:{Type2})  WHERE n2.ID = "{ID2}" 
            CREATE (n1)-[:INCLUDED]->(n2);
            '''

    qTest = f'''
            ######### Testing:#######################################
            match p= (n1)-[:INCLUDED]->(n2) return p limit 25;
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    myNeo.Neo4J_relationship_create(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//intraEntitiesRel''')
        file.write(f'''\n{qCreateEntity}\n\n''')


