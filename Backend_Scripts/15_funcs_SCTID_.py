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





def loadOCPSConcepts(driver , queryLocation, logHeader, Neo4J_import_fileName, LogID=""):
    print(logHeader)
    print('Creating Concept Nodes from CSV:\n')
    query = f''' LOAD CSV WITH HEADERS FROM \"file:///{Neo4J_import_fileName}\" as line 
    CALL {{ 
    with line'''



    for col in logHeader:

        if col == 'idx' or col == 'conceptId' or col == 'conceptCode' :
            column = f'''toInteger(line.{col})'''
        elif col in ['timestamp', 'start', 'end']:
            column = f'''datetime(line.{col})'''
        else:
            column = f'''line.''' + col

        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f''' 
            CREATE (s:Concept {{Log: "{LogID}",
            {col}: {column},'''
        elif (logHeader.index(col) == 0):
            newLine = f''' 
            CREATE (s:Concept {{ {col}: {column},'''
        else:
            newLine = f''' 
            {col}: {column},'''
        if (logHeader.index(col) == len(logHeader) - 1):
            newLine = f''' 
            {col}: {column} '''

        query = query + newLine

    finalQuery = query + f'''
          }})
    }} IN TRANSACTIONS ;
    '''

    testingQ = f'''
            #########Step3 Testing:######################################
            MATCH (s:Concept) 
            return s;

            MATCH (s:Concept) 
            return count(*) as count;
            #############################################################
            '''



    print(finalQuery)
    with driver.session() as session:
        Result=session.run(finalQuery).consume().counters
        myNeo.Neo4J_label_node_property(Result)
        print(testingQ)
        with open(queryLocation, 'a') as file:
            file.write(f'''//loadOCPSConcepts''')
            file.write(f'''\n{finalQuery}\n\n''')






def loadOCPSConceptsNew(tx , conceptId, conceptCode, termA_t1, termA_t2, termB, Semanti_tags, ConceptType, level, queryLocation):

    finalQuery = f'''  CREATE (s:Concept {{  conceptId: toInteger({conceptId}), conceptCode: toInteger({conceptCode}), termA: "{termA_t1}", termB: "{termB}",  Semanti_tags: "{Semanti_tags}",  ConceptType: "{ConceptType}", level: "{level}",  Log: "SNOMED_CT"}});'''

    testingQ = f'''
            #########Step3 Testing:######################################
            MATCH (s:Concept) 
            return s;

            MATCH (s:Concept) 
            return count(*) as count;
            #############################################################
            '''



    print(finalQuery)

    with open(queryLocation, 'a') as file:
        file.write(f'''//loadOCPSConcepts''')
        file.write(f'''\n{finalQuery}\n\n''')

    tx.run(finalQuery)



################################################################################
