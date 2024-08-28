import pandas as pd
import time, csv
from neo4j import GraphDatabase
import B02_neo4j as myNeo

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

###############################################################################################
# Neo4J Output massage functions

def Neo4J_relationship_massage(result):
    import ast
    output = ast.literal_eval(str(result))
    # print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["properties_set"]
        reltionship_num = output["relationships_created"]
        output_string = f'''
            Set {property_num} properties, created {reltionship_num} relationships
        '''
    return print(output_string)



################################## Step C1 ###########################################################


def deleteRelation(tx, relationTypes, queryLocation):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r:{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)
        with open(queryLocation, 'a') as file:
            file.write(f'''//deleteRelation''')
            file.write(f'''\n{qDeleteRelation}\n\n''')


def deleteAllRelations(tx, queryLocation):
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r;"
    print(qDeleteAllRelations)
    tx.run(qDeleteAllRelations)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteAllRelations''')
        file.write(f'''\n{qDeleteAllRelations}\n\n''')


def DeleteNodes(tx, nodeTypes, queryLocation):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)
        with open(queryLocation, 'a') as file:
            file.write(f'''//DeleteNodes''')
            file.write(f'''\n{qDeleteNodes}\n\n''')


def deleteAllNodes(tx, queryLocation):
    qDeleteAllNodes = "MATCH (n) DELETE n;"
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteAllNodes''')
        file.write(f'''\n{qDeleteAllNodes}\n\n''')


def deleteAllNodesandRel(tx, queryLocation):
    qDeleteAllNodes = "MATCH (n) DETACH DELETE n;"
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteAllNodesandRel''')
        file.write(f'''\n{qDeleteAllNodes}\n\n''')

def deletePartiallyRel(tx, rel, where, val, queryLocation):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePartiallyRel''')
        file.write(f'''\n{qDeleteRelation}\n\n''')


################################## Step C2 ###########################################################


def clearConstraint(tx, queryLocation):
    for x in tx.run("SHOW CONSTRAINTS;"):
        if x is not None:
            y = x["name"]
            z = x["labelsOrTypes"]
            #print(y)
            dropQ = "DROP CONSTRAINT " + y
            print(dropQ, " // for ", z[0])
            Result = tx.run(dropQ).consume().counters
            myNeo.Neo4J_removingConstraint(Result)
            with open(queryLocation, 'a') as file:
                file.write(f'''//clearConstraint''')
                file.write(f'''\n{dropQ}\n\n''')

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


################################## Step C3 ###########################################################

def createConstraint(tx,nodeTypesC, queryLocation):
    for i in range(len(nodeTypesC)):
        queryCreate = f'''CREATE CONSTRAINT FOR (z:{nodeTypesC[i]}) REQUIRE z.ID IS UNIQUE;'''
        print(queryCreate)
        Result = tx.run(queryCreate).consume().counters
        myNeo.Neo4J_creatingConstraint(Result)
        with open(queryLocation, 'a') as file:
            file.write(f'''//createConstraint''')
            file.write(f'''\n{queryCreate}\n\n''')

    qTest = f'''
                ######### Testing:#######################################
                :schema'
                ##############################################################
                '''

    print(qTest)



################################## Step C4 ###########################################################


def createLogNode(tx, log_id, queryLocation):
    print("")
    print("Inputs:")
    print("log_id=", log_id)
    print("")
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'
    with open(queryLocation, 'a') as file:
        file.write(f'''//createLogNode''')
        file.write(f'''\n{qCreateLog}\n\n''')

    qTest = f'''
            ######### Testing:#######################################
            MATCH (l:Log) return l;

            MATCH (l:Log) return count(*) as count;
            ##############################################################
            '''
    print(qCreateLog)


    Result = tx.run(qCreateLog).consume().counters
    myNeo.Neo4J_label_node_property(Result)
    print(qTest)

################################## Step C5 ###########################################################

# create events from CSV table: one event node per row, one property per column
def CreateEventNode(driver, logHeader, fileName, EntityIDColumnList,queryLocation, LogID="" ):
    print("")
    print("Inputs:")
    print("logHeader=", logHeader)
    print("fileName=", fileName)
    print("EntityIDColumnList=", EntityIDColumnList)
    print("LogID=", LogID)
    print("")

    print('Creating Event Nodes from CSV:\n')
    query = f''' LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line 
    CALL {{ 
    with line'''

    for col in logHeader:
        if col == 'idx' or col == 'row':
            column = f'''toInteger(line.{col})'''
        elif col in ['timestamp', 'start', 'end']:
            column = f'''datetime(line.{col})'''
        elif col in EntityIDColumnList or col == 'Activity_Properties_ID':
            column = f'''apoc.convert.fromJsonList(line.{col})'''
        else:
            column = 'line.' + col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f''' 
            CREATE (e:Event {{Log: "{LogID}",
            {col}: {column},'''
        elif (logHeader.index(col) == 0):
            newLine = f''' 
            CREATE (e:Event {{ {col}: {column},'''
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
            ######### Testing:######################################
            MATCH (e:Event) 
            return e;

            MATCH (e:Event) 
            return count(*) as count;
            #############################################################
            '''

    print(finalQuery)
    with open(queryLocation, 'a') as file:
        file.write(f'''//CreateEventNode''')
        file.write(f'''\n{finalQuery}\n\n''')

    with driver.session() as session:
        Result=session.run(finalQuery).consume().counters
        myNeo.Neo4J_label_node_property(Result)
        print(testingQ)

    #return finalQuery, testingQ

def CreateEventNodeNew(tx, actList, actProIdList, actSynList, actValdList, entityIdList, entityOriginList, eventList, timeList, queryLocation):
    print("")
    print("Inputs:")
    #print("actList=", actList)
    #print("actProIdList=", actProIdList)
    #print("actSynList=", actSynList)
    #print("actValdList=", actValdList)
    #print("entityIdList=", entityIdList)
    #print("entityOriginList=", entityOriginList)
    #print("eventList=", eventList)
    #print("timeList=", timeList)
    #print("")



    #print('Creating Event Nodes:\n')


    newLine1 = f''' Activity: "{actList}",'''
    #print(newLine1)

    newLine2 = f''' Activity_Properties_ID: apoc.convert.fromJsonList("{actProIdList}"), '''
    #print(newLine2)

    newLine3 = f''' Activity_Synonym: "{actSynList}",'''
    #print(newLine3)

    newLine4 = f''' Activity_Value_ID: "{actValdList}",'''
    #print(newLine4)

    newLine5 = f''' '''
    for i in range(len(entityIdList)):
        newLine = f''' Entity{i+1}_ID: apoc.convert.fromJsonList("{entityIdList[i]}")'''
        newLine5 = newLine + "," + newLine5
    #print(newLine5)

    newLine6 = f''' '''
    for i in range(len(entityOriginList)):
        newLine = f''' Entity{i+1}_Origin: "{entityOriginList[i]}"'''
        newLine6 = newLine + "," + newLine6
    #print(newLine6)

    newLine7 = f''' Event: "{eventList}",'''
    #print(newLine7)

    newLine8 = f''' Log: "EventLog",'''
    #print(newLine8)

    newLine9 = f''' timestamp: datetime("{timeList}")'''
    #print(newLine9)

    finalQuery = f'''  CREATE (p:Event {{ ''' + newLine1 + newLine2 +  newLine3 +  newLine4 +  newLine5 +  newLine6 +  newLine7 +   newLine8 +  newLine9 +   f'''}});'''


    testingQ = f'''
            ######### Testing:######################################
            MATCH (e:Event) 
            return e;

            MATCH (e:Event) 
            return count(*) as count;
            #############################################################
            '''

    print(finalQuery)
    with open(queryLocation, 'a') as file:
        file.write(f'''//CreateEventNode''')
        file.write(f'''\n{finalQuery}\n\n''')
    tx.run(finalQuery)
    '''
    try:
        tx.run(finalQuery)
        logger.info("Query executed successfully")
    except Exception as e:
        logger.info(f"Failed to execute query: {e}")
    '''

################################## Step C6 ###########################################################

def createEntitiesNode(tx, entity_id, entityCol, entityWhere, MainEntity, queryLocation):
    qCreateEntity = f'''
            MERGE (n:{MainEntity} {{
            EntityCol:"{entityCol}",          
            ID:toString("{entity_id}"),
            Category:"Absolute",
            Value:toString("{entity_id}")
            }});'''
    print(qCreateEntity)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createEntitiesNode''')
        file.write(f'''\n{qCreateEntity}\n\n''')

    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;

            MATCH (n:{MainEntity}) 
            where n.EntityCol="{entityCol}"
            return n;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)

################################## Step C7 ###########################################################


def createActivityNode(tx, act, syn, queryLocation):
    qCreateEC = f'''
            MERGE (n:Activity {{
            ID:"{act}", 
            Type:"Activity",
            Name:"{act}",
            Syn: "{syn}"
            }});'''
    print(qCreateEC)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createActivityNode''')
        file.write(f'''\n{qCreateEC}\n\n''')

    qTest = f'''
            ######### Testing:######################################
            MATCH (n:Activity) 
            RETURN n 
            #############################################################
            '''
    print(qTest)
    tx.run(qCreateEC)


################################## Step C8 ###########################################################


def createActivityPropertiesNode(tx, id, Name, Syn, featureID, queryLocation):
    qCreateEC = f'''
            MERGE (n:ActivityPropery {{
            ID:"{id}", 
            Type:"ActivityPropery",
            Name:"{Name}",
            Syn: "{Syn}",
            featureID: apoc.convert.fromJsonList("{featureID}")
            }});'''
    print(qCreateEC)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createActivityPropertiesNode''')
        file.write(f'''\n{qCreateEC}\n\n''')

    qTest = f'''
            ######### Testing:######################################
            MATCH (n:ActivityPropery) 
            RETURN n 
            #############################################################
            '''
    print(qTest)
    tx.run(qCreateEC)

################################## Step C9 ###########################################################

def link_log_events(tx, log_id, queryLocation):
    print("")
    print("Inputs:")
    print("log_id=", log_id)
    print("")
    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:HAS]->(e);'''
    print(qLinkEventsToLog)
    with open(queryLocation, 'a') as file:
        file.write(f'''//link_log_events''')
        file.write(f'''\n{qLinkEventsToLog}\n\n''')

    qTest = f'''
            ######### Testing:#######################################
            MATCH p=(l)-[:HAS]->(e)
            return p;
            ##############################################################
            '''

    print(qTest)

    tx.run(qLinkEventsToLog)



################################## Step C10 ###########################################################

def link_events_Entities(tx, entity_id, entityCol, entityWhere, MainEntity, queryLocation):
    qCorrelate = f'''
            MATCH (e:Event) {entityWhere} and "{entity_id}" in e.{entityCol}
            MATCH (n:{MainEntity} {{EntityCol: "{entityCol}" }}) WHERE n.ID = "{entity_id}"
            CREATE (e)-[:CORR{{Scenario:"1"}}]->(n)'''
    print(qCorrelate)
    with open(queryLocation, 'a') as file:
        file.write(f'''//link_events_Entities''')
        file.write(f'''\n{qCorrelate}\n\n''')

    qTest = f''' 
            ######### Testing:#######################################         
            MATCH p=(e)-[:CORR]->(n)
            WHERE n.EntityCol= "{entityCol}"and n.ID="{entity_id}"
            return p;
            ##############################################################
            '''

    print(qTest)
    tx.run(qCorrelate)





################################## Step C11 ###########################################################

def link_events_Activity(tx, cond1, cond2, act, syn, queryLocation):
    qLinkEventToClass = f'''
            MATCH ( c : Activity ) WHERE c.Name ="{act}" and c.Syn="{syn}"
            MATCH ( e : Event ) where {cond1} and {cond2}
            CREATE ( e ) -[:OBSERVED{{Scenario:"1"}}]-> ( c )'''
    print(qLinkEventToClass)
    with open(queryLocation, 'a') as file:
        file.write(f'''//link_events_Activity''')
        file.write(f'''\n{qLinkEventToClass}\n\n''')


    qTest = f'''
            ######### Testing:######################################
            MATCH l=( e ) -[:OBSERVED]-> ( c ) 
            RETURN l
            #############################################################
            '''
    print(qTest)
    tx.run(qLinkEventToClass)



################################## Step C12 ###########################################################

def link_events_ActivityProperty(tx, queryLocation):
    qLinkEventToClass = f'''
            MATCH ( c : ActivityPropery )
            MATCH ( e : Event ) 
            where e.Activity_Properties_ID = c.featureID and e.Activity=c.Name and e.Activity_Synonym=c.Syn
            CREATE ( e ) -[:MONITORED{{Scenario:"1"}}]-> ( c )'''
    print(qLinkEventToClass)
    with open(queryLocation, 'a') as file:
        file.write(f'''//link_events_ActivityProperty''')
        file.write(f'''\n{qLinkEventToClass}\n\n''')

    qTest = f'''
            ######### Testing:######################################
            MATCH l=( e ) -[:MONITORED]-> ( c ) 
            RETURN l
            #############################################################
            '''
    print(qTest)
    tx.run(qLinkEventToClass)





