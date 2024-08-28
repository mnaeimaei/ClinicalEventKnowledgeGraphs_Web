import pandas as pd
import time, csv
from neo4j import GraphDatabase


###############################################################################################
# Neo4J Output massage functions

import B02_neo4j as myNeo

################################## Step V1 ###########################################################


def deleteRelation(tx, relationTypes, queryLocation):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)
        with open(queryLocation, 'a') as file:
            file.write(f'''//deleteRelation''')
            file.write(f'''\n{qDeleteRelation}\n\n''')

def DeleteNodes(tx, nodeTypes, queryLocation):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)
        with open(queryLocation, 'a') as file:
            file.write(f'''//DeleteNodes''')
            file.write(f'''\n{qDeleteNodes}\n\n''')

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







################################## Step V2 ###########################################################

def modifyEntities(tx, alias, ID, MainEntity, queryLocation):
    qModifyEntities = f'''
            MATCH (n:{MainEntity}) WHERE n.ID = "{ID}"
            SET
                n.Type= "{MainEntity}",
                n.uID="{alias}"+toString("{ID}")+toString("{ID}")
                '''

    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;


            MATCH (n:{MainEntity}) 
            where n.Type: "{MainEntity}",
            return n;
            ##############################################################
            '''


    print(qModifyEntities)
    Result = tx.run(qModifyEntities).consume().counters
    myNeo.Neo4J_properties_set(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//modifyEntities''')
        file.write(f'''\n{qModifyEntities}\n\n''')

################################## Step V3 ###########################################################



def createReifiedEntities(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qCreateReifiedEntity = f'''

            Create (n:{MainEntity} {{ 
                Category:"Relative" ,          
                uID:"{alias}"+toString("{entityID1}")+toString("{entityID2}")+toString("{entityID1}")+toString("{entityID2}"),
                Type: "{relation_type}",
                ID:toString("{entityID1}")+"_"+toString("{entityID2}") }});'''


    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;

            MATCH (n:{MainEntity}) 
            where n.Name: "{relation_type}
            return n;

            MATCH (n:{MainEntity}) 
            where n.Source: "{MainEntity}",
            return n;
            ##############################################################
            '''



    print(qCreateReifiedEntity)
    Result = tx.run(qCreateReifiedEntity).consume().counters
    myNeo.Neo4J_label_node_property(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//createReifiedEntities''')
        file.write(f'''\n{qCreateReifiedEntity}\n\n''')




################################## Step V4 ###########################################################

def entities_with_diff_ID_relationships(tx, Alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qEntities_with_diff_ID_relationships = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:{MainEntity} ) WHERE n1.ID="{entityID1}" AND n1.Type="{MainEntity}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:{MainEntity} ) WHERE n2.ID="{entityID2}" AND n2.Type="{MainEntity}"
            AND n1 <> n2 AND n1.Type=n2.Type
            WITH DISTINCT n1,n2
            CREATE ( n2 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n1 )'''


    qTest = f'''
            ######### Testing:######################################
            MATCH ( n1 : {MainEntity} ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:{MainEntity} )
            return n1,n2;
            ##############################################################
            '''



    print(qEntities_with_diff_ID_relationships)
    Result = tx.run(qEntities_with_diff_ID_relationships).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//entities_with_diff_ID_relationships''')
        file.write(f'''\n{qEntities_with_diff_ID_relationships}\n\n''')

################################## Step V5 ###########################################################

def RelatingReifiedEntitiesAndEntities(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qRelatingReifiedEntitiesAndEntities = f'''
            MATCH ( n1 : {MainEntity} ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:{MainEntity} )
            MATCH ( en : {MainEntity} ) where 
            en.uID="{alias}"+toString("{entityID1}")+toString("{entityID2}")+toString("{entityID1}")+toString("{entityID2}")
            AND en.Type= "{relation_type}"
            AND en.ID=toString("{entityID1}")+"_"+toString("{entityID2}") 
            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (en) -[:REL {{Type:"Reified"}}]-> (n2)'''


    qTest = f'''
            ######### Testing:######################################
            MATCH p=(n1:{MainEntity}) <-[:REL]- (r:{MainEntity}) -[:REL]-> (n2:{MainEntity})
            where r.Type="{relation_type}"
            return p;


            MATCH p=(n1:{MainEntity}) <-[:REL]- (r:{MainEntity}) -[:REL]-> (n2:{MainEntity})
            return p;
            ##############################################################
            '''



    print(qRelatingReifiedEntitiesAndEntities)
    Result = tx.run(qRelatingReifiedEntitiesAndEntities).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//RelatingReifiedEntitiesAndEntities''')
        file.write(f'''\n{qRelatingReifiedEntitiesAndEntities}\n\n''')


################################## Step V6 ###########################################################

def correlate_ReifiedEntities_to_Event(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qCorrelate_ReifiedEntities_to_Event = f'''
            MATCH (e:Event) -[:CORR]-> (n:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            CREATE (e)-[:CORR]->(en)'''

    qTest = f'''
            ######### Testing:######################################
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
            #############################################################
            '''



    print(qCorrelate_ReifiedEntities_to_Event)
    Result = tx.run(qCorrelate_ReifiedEntities_to_Event).consume().counters
    myNeo.Neo4J_relationship_create(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//correlate_ReifiedEntities_to_Event''')
        file.write(f'''\n{qCorrelate_ReifiedEntities_to_Event}\n\n''')



################################## Step V7 ###########################################################

def createDF(tx, Source, Type, queryLocation):
    qCreateDF = f'''
        MATCH ( n : {Source} ) WHERE n.Type="{Type}"
        MATCH ( n ) <-[:CORR]- ( e )
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second, n.ID as NewID
        MERGE ( first ) -[df:DF {{Type:"{Type}",Name:"{Source}", Category:"woProperty"}} ]->( second )
        ON CREATE SET df.ID=NewID 
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH  p=(first)-[df:DF]-> (second)
            where df.Type="{Type}"
            return p;
            #############################################################
            '''


    print(qCreateDF)
    Result = tx.run(qCreateDF).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//qCreateDF''')
        file.write(f'''\n{qCreateDF}\n\n''')



################################## Step V8 ###########################################################

def deletePuluted_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qPuluted1 = f'''
            MATCH (e1:Event)-[df:DF{{Type: "{relation_type}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{Type: "{MainEntity}" }}]-> (e2:Event)
            DELETE df'''


    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
                After:
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
            #############################################################
            '''

    print(qPuluted1)
    Result = tx.run(qPuluted1).consume().counters
    myNeo.Neo4J_relationship_delete(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePuluted_Reified_DF''')
        file.write(f'''\n{qPuluted1}\n\n''')


################################## Step V9 ###########################################################

def deleteWrong_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qPuluted1 = f'''
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp  and df3.Type="{relation_type}"
            DELETE df3
            '''


    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp
            return distinct n1.ID,e1.Event,n2.ID,e2.Event,e2.timestamp,n3.ID,e3.Event,e3.timestamp;
                
                After:
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp
            return distinct n1.ID,e1.Event,n2.ID,e2.Event,e2.timestamp,n3.ID,e3.Event,e3.timestamp;
            #############################################################
            '''

    print(qPuluted1)
    Result = tx.run(qPuluted1).consume().counters
    myNeo.Neo4J_relationship_delete(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteWrong_Reified_DF''')
        file.write(f'''\n{qPuluted1}\n\n''')


################################## Step V10 ###########################################################

def deleteExtra_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qPuluted2 = f'''
            MATCH (n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where n1.ID="{entityID2}" and n2.ID="{entityID1}"
            DELETE df;           
            '''

    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH p=(n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where (n1.ID="{entityID2}" and n2.ID="{entityID1}") or (n1.ID="{entityID1}" and n2.ID="{entityID2}") 
            return p;
            
                After:
            MATCH p=(n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where (n1.ID="{entityID2}" and n2.ID="{entityID1}") or (n1.ID="{entityID1}" and n2.ID="{entityID2}") 
            return p;
            #############################################################
            '''

    print(qPuluted2)
    Result = tx.run(qPuluted2).consume().counters
    myNeo.Neo4J_relationship_delete(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deleteExtra_Reified_DF''')
        file.write(f'''\n{qPuluted2}\n\n''')

################################## Step V11 ###########################################################

def deletePolluted_CoRR_Reified_Events(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qPuluted3 = f'''

            MATCH (e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH (e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            delete c1,c2;
        
            '''

    qPuluted32 = f'''

            MATCH p=(e1)-[c1:CORR]->(n1) where n1.Type="{relation_type}" 
            MATCH q=(e1) -[df:DF]-> (e2) where df.Type<> "{relation_type}" 
            delete c1;

            '''

    qTest = f'''
            ######### Testing:######################################
              Before:
            MATCH p1=(e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH p2=(e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            return p1,p2
            
             After:
            MATCH p1=(e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH p2=(e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            return p1,p2

            #############################################################
            '''

    print(qPuluted3)
    Result = tx.run(qPuluted3).consume().counters
    myNeo.Neo4J_relationship_delete(Result)
    print(qPuluted32)
    Result = tx.run(qPuluted32).consume().counters
    myNeo.Neo4J_relationship_delete(Result)


    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePolluted_CoRR_Reified_Events''')
        file.write(f'''\n{qPuluted3}\n\n''')



################################# Step V12 ###########################################################

def deletePolluted_CoRR_Reified_Events_2(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
    qPuluted4 = f'''

            MATCH  (en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )

            CREATE (e1)-[:CORR]->(en)<-[:CORR]-(e2);  '''

    qTest = f'''
            ######### Testing:######################################
              Before:
              MATCH  p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            return p
            
             After:
            MATCH  p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            return p
              

            #############################################################
            '''

    print(qPuluted4)
    Result = tx.run(qPuluted4).consume().counters
    myNeo.Neo4J_relationship_create(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//deletePolluted_CoRR_Reified_Events_2''')
        file.write(f'''\n{qPuluted4}\n\n''')

################################## Step V13 ###########################################################

def wrong_reified(tx, alias, relation_type, entityID1, entityID2, MainEntity, queryLocation):
        qPuluted5 = f'''
                OPTIONAL MATCH   (en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n1:{MainEntity}{{ID:"{entityID1}"}})
               -[r:REL ]->(n2:{MainEntity}{{ID:"{entityID2}"}})<-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
                
                OPTIONAL MATCH    p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n1:{MainEntity})<-[:CORR]- 
                (e1:Event) -[df:DF ]->   (e2:Event)   
                -[:CORR]-> (n2:{MainEntity}) <-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
                where df.Type="{relation_type}"
                
                with p,r,r1,r2,en
                where p is null
                delete r,r1,r2,en
                '''


        qTest = f'''
                #########  Testing: ######################################
                Before/After:
                MATCH (n1:{MainEntity}{{ID:"{entityID1}"}}) -[r:REL ]->(n2:{MainEntity}{{ID:"{entityID2}"}})
                MATCH p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
                (e2:Event) -[df:DF ]->   (e1:Event)   
                -[:CORR]-> (n1:{MainEntity}) <-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
                where df.Type<>"{relation_type}"
                return p
                #################################################################
                '''


        print(qPuluted5)
        Result = tx.run(qPuluted5).consume().counters
        myNeo.Neo4J_relationship_and_Node_delete(Result)
        print(qTest)
        with open(queryLocation, 'a') as file:
            file.write(f'''//wrong_reified''')
            file.write(f'''\n{qPuluted5}\n\n''')


################################## Step V14 ###########################################################

def aggregateDF_Absolute(tx, Ent, EntIDbased, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Absolute = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{Ent}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type AND n.ID = df.ID  AND n.Type ="{EntIDbased}"  
        WITH n.Type as EType,c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{Type:"Absolute" , count:df_freq , En1_ID:IDT , En2_ID:IDT , En1:"{Ent}" , En2:"{Ent}" , Category:"woProperty"}}]-> ( c2 ) 
        '''



    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_Absolute)
    Result = tx.run(qCreateDF_Absolute).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_Absolute''')
        file.write(f'''\n{qCreateDF_Absolute}\n\n''')


################################## Step V15 ###########################################################

def aggregateDF_Relative(tx, En1, En2, eID, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Relative = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df1:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df2:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n1:{En1}) <-[:CORR]- (e2)
        MATCH (e1) -[:CORR] -> (n2:{En2}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type 
        AND n1.Type = df1.Type  AND n1.ID = df1.ID
        AND n2.Type = df2.Type  AND n2.ID = df2.ID AND n2.ID="{eID}"
        WITH c1,count(df1) AS df_freq,c2, n1.ID as IDT1, n2.ID as IDT2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"Relative" , count:df_freq , En1_ID:IDT1 ,En2_ID:IDT2 , En1:"{En1}" , En2:"{En2}" , Category:"woProperty"}}]-> ( c2 ) 
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            #############################################################
            '''
    print(qCreateDF_Relative)
    Result = tx.run(qCreateDF_Relative).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_Relative''')
        file.write(f'''\n{qCreateDF_Relative}\n\n''')

################################## Step V16 ###########################################################

def aggregateDF_All(tx, list, firstItem, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_All''')
        file.write(f'''\n{qCreateDF_All}\n\n''')


def aggregateDF_All_inactiveID(tx, list, firstItem, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_All_inactiveID''')
        file.write(f'''\n{qCreateDF_All}\n\n''')


def aggregateDF_All_activeID(tx, list, firstItem, entityID, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}  and n.ID in {entityID} 
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_All_activeID''')
        file.write(f'''\n{qCreateDF_All}\n\n''')


################################## Step V17 ###########################################################

def relEntity(tx,  item0ID, item0, item1, item1ID1,item1ID2, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateAggEntity = f'''
        MATCH (a1:{item1}) where a1.ID="{item1ID1}"
        MATCH (a2:{item1}) where a2.ID="{item1ID2}"
        MERGE ( a1 ) -[:DF_E {{Type:"One" , Base:"{item1}",  Source:"{item0}" , sourceID:"{item0ID}"  }}]-> ( a2 ) 

        '''


    qTest = f'''
            ######### Testing:######################################
            match p=( a1 ) -[:DF_E {{Type:"One" , Base:"{item1}",  Source:"{item0}"   }}]-> ( a2 )
            return p
            #############################################################
            '''
    print(qCreateAggEntity)
    Result = tx.run(qCreateAggEntity).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//relEntity''')
        file.write(f'''\n{qCreateAggEntity}\n\n''')




################################## Step V18 ###########################################################

def relEntityLower(tx, item1, item3, index,source,SourceID, id1, id2, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateTwoEn = f'''
        {item3}<-[:DF_E]- (n:{item1})
        WHERE n.ID="{id1}"
        MERGE ( n ) -[:DF_E {{Type:"Two" , Base:"{item1}",  Source:"{source}" , sourceID:"{SourceID}"  }}]-> ( {index} ) ;
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH p1=( n ) -[:DF_E {{Type:"Two" , Base:"{item1}",  Source:"{source}" , sourceID:"{SourceID}"  }}]-> ( {index} )
            RETURN p1 ;
            #############################################################
            '''
    print(qCreateTwoEn)
    Result = tx.run(qCreateTwoEn).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//relEntityLower''')
        file.write(f'''\n{qCreateTwoEn}\n\n''')

################################## Step V19 ###########################################################

def DF_Propery(tx, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(e1)-[d:DF]->(e2)
        MATCH p2=(e1)-[a:Assign]->(f)
        WITH e2,d.ID as ID, d.Name as Name, d.Type as Type ,f
        MERGE ( f ) -[:DF {{ID:ID, Name:Name , Type:Type , Category:"wProperty" }}]-> ( e2 ) 

        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( e2 ) -[:DF ]-> ( f )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//DF_Propery''')
        file.write(f'''\n{qCreateDF_All}\n\n''')



################################## Step 20 ###########################################################

def aggregateDF_AbsoluteProperty(tx, Ent, EntIDbased, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Absolute = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{Ent}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type AND n.ID = df.ID  AND n.Type ="{EntIDbased}"  
        WITH n.Type as EType,c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AbsoluteProperty" , count:df_freq , En1_ID:IDT , En2_ID:IDT , En1:"{Ent}" , En2:"{Ent}" , Category:"wProperty"}}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_Absolute)
    Result = tx.run(qCreateDF_Absolute).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_AbsoluteProperty''')
        file.write(f'''\n{qCreateDF_Absolute}\n\n''')

def DF_AbsolutePropery(tx, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"AbsoluteProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 
            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//DF_AbsolutePropery''')
        file.write(f'''\n{qCreateDF_All}\n\n''')




################################## Step 21 ###########################################################

def aggregateDF_RelativeProperty(tx, En1, En2, eID, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Relative = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df1:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df2:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n1:{En1}) <-[:CORR]- (e2)
        MATCH (e1) -[:CORR] -> (n2:{En2}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type 
        AND n1.Type = df1.Type  AND n1.ID = df1.ID
        AND n2.Type = df2.Type  AND n2.ID = df2.ID AND n2.ID="{eID}"
        WITH c1,count(df1) AS df_freq,c2, n1.ID as IDT1, n2.ID as IDT2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"RelativeProperty" , count:df_freq , En1_ID:IDT1 ,En2_ID:IDT2 , En1:"{En1}" , En2:"{En2}" , Category:"wProperty" }}]-> ( c2 ) 
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            #############################################################
            '''
    print(qCreateDF_Relative)
    Result = tx.run(qCreateDF_Relative).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_RelativeProperty''')
        file.write(f'''\n{qCreateDF_Relative}\n\n''')





def DF_RelativePropery(tx, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"RelativeProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 

            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//DF_RelativePropery''')
        file.write(f'''\n{qCreateDF_All}\n\n''')



################################## Step 22 ###########################################################

def aggregateDF_AllProperty(tx, list, firstItem, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 

        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_AllProperty''')
        file.write(f'''\n{qCreateDF_All}\n\n''')


def aggregateDF_AllProperty_inactiveID(tx, list, firstItem, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_AllProperty_inactiveID''')
        file.write(f'''\n{qCreateDF_All}\n\n''')



def aggregateDF_AllProperty_activeID(tx, list, firstItem, entityID, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}  and n.ID in {entityID} 
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregateDF_AllProperty_activeID''')
        file.write(f'''\n{qCreateDF_All}\n\n''')




def DF_AllPropery(tx, queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"AllProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 

            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//DF_AllPropery''')
        file.write(f'''\n{qCreateDF_All}\n\n''')


################################## Step Not Used 1 ###########################################################

def aggregate3Entity2(tx,property1, property2, en1, en2, en3, rel, ID_A , ID_B , queryLocation):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateTwoEn_2 = f'''
        MATCH p1=(s1:{en2})<-[:CORR]-(e1:Event)-[:CORR]->(v1:{en1})-[:ATTRIBUTES]->(d1:{en3}) 
        where d1.Name="{property1}" and s1.Category="Absolute"   and v1.Category="Absolute" and v1.ID="{ID_A}"
        MATCH p2=(s1:{en2})<-[:CORR]-(e2:Event)-[:CORR]->(v2:{en1})-[:ATTRIBUTES]->(d2:{en3}) 
        where d2.Name="{property2}"   and v2.ID="{ID_B}"
        WITH   distinct s1.ID as SID , d1 ,d2
        MERGE ( d1 ) -[rel2:DF_E {{Type:"threeEntity" , count:"1" , En1_ID:SID , En1:"{en2}" , En2:"{property2}",  En2_ID:"{rel}" }}]-> ( d2 ) ;
        '''


    qTest = f'''
            ######### Testing:######################################
            MATCH p1=(s1:{en2})<-[:CORR]-(e1:Event)-[:CORR]->(v1:{en1})-[:ATTRIBUTES]->(d1:{en3}) 
            where d1.Name="{property1}" and s1.Category="Absolute"   and v1.Category="Absolute" and v1.ID="{ID_A}"
            MATCH p2=(s1:{en2})<-[:CORR]-(e2:Event)-[:CORR]->(v2:{en1})-[:ATTRIBUTES]->(d2:{en3}) 
            where d2.Name="{property2}"   and v2.ID="{ID_B}"
            return   distinct s1.ID as SID , d1 ,d2;
            #############################################################
            '''
    print(qCreateTwoEn_2)
    Result = tx.run(qCreateTwoEn_2).consume().counters
    myNeo.Neo4J_relationship_massage(Result)
    print(qTest)
    with open(queryLocation, 'a') as file:
        file.write(f'''//aggregate3Entity2''')
        file.write(f'''\n{qCreateTwoEn_2}\n\n''')
