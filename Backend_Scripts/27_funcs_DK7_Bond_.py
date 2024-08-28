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





def Event_Scenario_1(tx, Activity_Value_ID , DisorderID,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Disorder ) where p.ID="{DisorderID}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_1''')
        file.write(f'''\n{qLinkSCTs}\n\n''')



def Event_Scenario_2(tx, Activity_Value_ID , icd_Code,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{icd_Code}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_2''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_3(tx, Activity_Value_ID , icd_code_up,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{icd_code_up}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_3''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_4(tx, Activity_Value_ID , Snomed,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={Snomed}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_4''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_5(tx, Activity_Value_ID , Snomed_up,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={Snomed_up}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_5''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_6(tx, Activity_Value_ID , snomed_up_common,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={snomed_up_common}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_6''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_7(tx, Activity_Value_ID , specificICD,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{specificICD}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_7''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def Event_Scenario_8(tx, Activity_Value_ID , specificSCT,Scenario, queryLocation):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={specificSCT}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''

    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Event_Scenario_8''')
        file.write(f'''\n{qLinkSCTs}\n\n''')




