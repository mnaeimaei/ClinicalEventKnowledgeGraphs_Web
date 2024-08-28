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


def Activity_OCPS(tx, Activity,	Activity_Synonym,	SCTID,	SCTCode, queryLocation):

    qLinkSCTs = f''' 
            MATCH ( c:Activity ) where c.Name="{Activity}" and c.Syn="{Activity_Synonym}"  
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( c ) -[:MAPPED_TO]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)
    with open(queryLocation, 'a') as file:
        file.write(f'''//Activity_OCPS''')
        file.write(f'''\n{qLinkSCTs}\n\n''')


def ActivityProperty_OCPS(tx, Activity,	Activity_Synonym,	SCTID,	SCTCode, queryLocation):

    qLinkSCTs = f''' 
            MATCH ( c:ActivityPropery ) where c.Name="{Activity}" and c.Syn="{Activity_Synonym}"  
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( c ) -[:MAPPED_TO]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)
    with open(queryLocation, 'a') as file:
        file.write(f'''//ActivityProperty_OCPS''')
        file.write(f'''\n{qLinkSCTs}\n\n''')

################################################################################


