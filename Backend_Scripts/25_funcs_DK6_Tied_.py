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

def deletePartiallyRel(tx, rel, where, val, queryLocation):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePartiallyRel''')
        file.write(f'''\n{qDeleteRelation}\n\n''')


def deletePartRel(tx, queryLocation):
    qDeleteAllNodes = f'''MATCH (e)-[c:CORR]->(r) where r.Category="Relative" DELETE c;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePartRel''')
        file.write(f'''\n{qDeleteAllNodes}\n\n''')



def deletePartNode(tx,EntityItems, queryLocation):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Relative" delete n;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePartNode''')
        file.write(f'''\n{qDeleteAllNodes}\n\n''')


def deleteProperty(tx,EntityItems, queryLocation):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Absolute" remove n.uID, n.Type;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteProperty''')
        file.write(f'''\n{qDeleteAllNodes}\n\n''')






def Activity_OCPS(tx, DomainName,	SCTID, SCTCode, queryLocation):

    qLinkSCTs = f''' 
            MATCH ( f:Domain ) where f.Name="{DomainName}"   
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( f ) -[:TIED]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Activity_OCPS''')
        file.write(f'''\n{qLinkSCTs}\n\n''')



def Domain_Scenario_1(tx, Activity, Activity_Synonym , Domain, queryLocation):
    qLinkSCTs = f''' 
            
            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (d:Domain) WHERE d.Name ="{Domain}"
            CREATE (a)-[:TYPE_OF{{Scenario:"1"}}]->(d);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_1''')
        file.write(f'''\n{qLinkSCTs}\n\n''')



def Domain_Scenario_2(tx, Activity, Activity_Synonym , conceptId, conceptCode, queryLocation):
    qLinkSCTs = f''' 
            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"2"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_2''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Domain_Scenario_3(tx, Activity, Activity_Synonym , conceptId, conceptCode, queryLocation):
    qLinkSCTs = f''' 
            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"3"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_3''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Domain_Scenario_1_Proprty(tx, Activity, Activity_Synonym, Domain, queryLocation):
    qLinkSCTs = f''' 

            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (d:Domain) WHERE d.Name ="{Domain}"
            CREATE (a)-[:TYPE_OF{{Scenario:"1"}}]->(d);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_1_Proprty''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Domain_Scenario_2_Proprty(tx, Activity, Activity_Synonym, conceptId, conceptCode, queryLocation):
    qLinkSCTs = f''' 
            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"2"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_2_Proprty''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Domain_Scenario_3_Proprty(tx, Activity, Activity_Synonym, conceptId, conceptCode, queryLocation):
    qLinkSCTs = f''' 
            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"3"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Domain_Scenario_3_Proprty''')
        file.write(f'''\n{qLinkSCTs}\n\n''')
