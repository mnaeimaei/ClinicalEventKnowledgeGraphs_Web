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


def admTreated_Fun(tx, admID, disorderName, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admTreated_Fun''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def admNotTreated_Fun(tx,  admID, disorderName, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admNotTreated_Fun''')
        file.write(f'''\n{qLinkSCTs}\n\n''')

def admNew_Fun(tx,  admID, disorderName, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admNew_Fun''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def admMulti_Value(tx, admID, value, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)
            Where a.ID="{admID}"
            SET m.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admMulti_Value''')
        file.write(f'''\n{qLinkSCTs}\n\n''')

def admTreated_Value(tx, admID, value, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"

            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admTreated_Value''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def admNotTreated_Value(tx,  admID, value, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admNotTreated_Value''')
        file.write(f'''\n{qLinkSCTs}\n\n''')

def admNew_Value(tx,  admID, value, queryLocation):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTesting)
    with open(queryLocation, 'a') as file:
        file.write(f'''//admNew_Value''')
        file.write(f'''\n{qLinkSCTs}\n\n''')
