import pandas as pd
import time, csv
from neo4j import GraphDatabase


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


def Potential_OCPS(tx, icd_code, SCTID, queryLocation):



    qLinkSCTs = f''' 
            MATCH ( s:Clinical ) where s.icdCode="{icd_code}"
            MATCH ( n:Concept ) where n.conceptId={SCTID}
            CREATE ( s ) -[:CONNECTED_TO]-> ( n )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Potential_OCPS''')
        file.write(f'''\n{qLinkSCTs}\n\n''')





