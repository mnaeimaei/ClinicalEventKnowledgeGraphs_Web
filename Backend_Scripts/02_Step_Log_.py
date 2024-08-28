import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os

import B02_base as cl2
import C02_Step1_Log_prepare1 as cl4
import C03_funcs2_Log_importingNeo4J1 as cl5
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla

driver=cl4.driver
dataSet=cl4.ED_dataSet
Perf_file_path = cl4.Perf_file_path



##################################################
userDirectory = f"../Data/registration/0_username.txt"
userPath = os.path.realpath(userDirectory)
with open(userPath, 'r') as file:
    for line in file:
        username = line
##################################################


medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '01_EventLog'
if not os.path.exists(outDir):
    os.mkdir(outDir)







selenium = cla.selenium

print(" ")
print("---------------------------------------- Pre Step C1,2,3 -----------------------------------------------------------------------------------")

EntityLists=cl4.EntityLists

############PART C ##########################################
relationTypesC = ["HAS", "CORR", "OBSERVED", "MONITORED"]
nodeTypesC = ["Log", "Event", "Activity", "ActivityPropery"]
nodeTypesC.extend(EntityLists)

############PART D ##########################################
nodeTypesD = []
relationTypesD = ["ATTRIBUTES"]


############PART E ##########################################
nodeTypesE = ["Feature"]
relationTypesE = ["Assign"]


############PART F ##########################################
nodeTypesF = ["Domain"]

############PART G ##########################################
nodeTypesG = []
relationTypesG = []


############PART H ##########################################
nodeTypesH = ["Clinical"]

############PART I ##########################################
nodeTypesI = ["Concept"]
relationTypesI = ["ANCESTOR_OF"]

############PART DK2 ##########################################

relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

############PART DK ##########################################
relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
relTypePartially = ["CORR", "Scenario", "2"]

############PART V ##########################################
relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

nodeTypes=nodeTypesC+nodeTypesD+nodeTypesE+nodeTypesF+nodeTypesG+nodeTypesH + nodeTypesI
relationTypes=relationTypesC+relationTypesD+relationTypesE+relationTypesG+relationTypesI+relationTypesDK+relationTypesV+relationTypesDK2

print(nodeTypes)
print(relationTypes)

print(" ")
print("---------------------------------------- Step C1 -----------------------------------------------------------------------------------")




step_ClearDB=True
if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    stepName='StepC1 - Clearing DB...'
    print('                      ')
    print(stepName)

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    start = time.time()

    with driver.session() as session:
        session.execute_write(cl5.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2],graphviz_QueryLocationQ1)
        session.execute_write(cl5.deleteRelation, relationTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.deleteAllRelations,graphviz_QueryLocationQ1)
        session.execute_write(cl5.DeleteNodes, nodeTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.deleteAllNodes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.deleteAllNodesandRel,graphviz_QueryLocationQ1)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)




print(" ")
print("---------------------------------------- Step C2 -----------------------------------------------------------------------------------")

step_ClearConstraints=True
if step_ClearConstraints:
    stepName='StepC2 - Droping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.clearConstraint, graphviz_QueryLocationQ2)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)


print(" ")
print("---------------------------------------- Step C3 -----------------------------------------------------------------------------------")

step_createConstraint=True
if step_createConstraint:
    stepName='StepC3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.createConstraint,nodeTypesC, graphviz_QueryLocationQ3)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)

print(" ")
print("---------------------------------------- Step C4 -----------------------------------------------------------------------------------")


step_createLog=True
if step_createLog:
    stepName='StepC4 - Creating Log Node...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')


    with driver.session() as session:
        session.execute_write(cl5.createLogNode, dataSet,graphviz_QueryLocationQ4)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)

'''
print(" ")
print("---------------------------------------- Step C5 -----------------------------------------------------------------------------------")

header_ED=cl4.header_ED
ED_Neo4JImport_Event_FileName=cl4.ED_Neo4JImport_Event_FileName
EntityIDColumnList=cl4.EntityIDColumnList


step_LoadEventsFromCSV=True
if step_LoadEventsFromCSV:
    stepName='StepC5 - Creating Event Nodes ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q5"
    graphviz_QueryLocationQ5 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ5, 'w') as file:
        file.write(f'''''')


    cl5.CreateEventNode(driver, header_ED, ED_Neo4JImport_Event_FileName,EntityIDColumnList, graphviz_QueryLocationQ5, dataSet)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)

'''
print(" ")
print("---------------------------------------- Step C5 New -----------------------------------------------------------------------------------")


actList=cl4.actList
actProIdList=cl4.actProIdList
actSynList=cl4.actSynList
actValdList=cl4.actValdList
entityIdList=cl4.entityIdList
entityOriginList=cl4.entityOriginList
eventList=cl4.eventList
timeList=cl4.timeList


step_LoadEventsFromCSV=True
if step_LoadEventsFromCSV:
    stepName='StepC5 - Creating Event Nodes ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q5"
    graphviz_QueryLocationQ5 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ5, 'w') as file:
        file.write(f'''''')

    for item1, item2, item3, item4, item5, item6, item7, item8 in zip(actList, actProIdList, actSynList, actValdList, entityIdList, entityOriginList, eventList, timeList):
        with driver.session() as session:
            session.execute_write(cl5.CreateEventNodeNew, item1, item2, item3, item4, item5, item6, item7, item8, graphviz_QueryLocationQ5)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)

print(" ")
print("---------------------------------------- Step C6 -----------------------------------------------------------------------------------")
model_entities=cl4.model_entities

step_createEntities=True
if step_createEntities:
    stepName='StepC6 - Creating Entities Node...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q6"
    graphviz_QueryLocationQ6 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ6, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")



    for entity in model_entities:
        with driver.session() as session:
            session.execute_write(cl5.createEntitiesNode, entity[0], entity[1], entity[2], entity[3], graphviz_QueryLocationQ6)
            print(f'\n     *{entity[3] + str(entity[0])} entity nodes done')


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path,start,end,stepName)


print(" ")
print("---------------------------------------- Step C7 -----------------------------------------------------------------------------------")


actNode=cl4.actNode

step_createActivityClasses=True
if step_createActivityClasses:
    stepName='StepC7 - Creating activities Nodes  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q7"
    graphviz_QueryLocationQ7 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ7, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("actNode=",actNode)
    print("")
    for item in actNode:
        with driver.session() as session:
            session.execute_write(cl5.createActivityNode, item[0], item[1], graphviz_QueryLocationQ7)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step C8 -----------------------------------------------------------------------------------")


actNodeWithID=cl4.actNodeWithID

step_createActivityPropertiesClasses=True
if step_createActivityPropertiesClasses:
    stepName='StepC7 - Creating activities properties Nodes  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q8"
    graphviz_QueryLocationQ8 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ8, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("actNodeWithID=",actNodeWithID)
    print("")
    for item in actNodeWithID:
        with driver.session() as session:
            session.execute_write(cl5.createActivityPropertiesNode, item[0], item[1],item[2], item[3], graphviz_QueryLocationQ8)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step C9 -----------------------------------------------------------------------------------")


step_link_log_evnts=True
if step_link_log_evnts:
    stepName='StepC9 - Linking log to events...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q9"
    graphviz_QueryLocationQ9 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ9, 'w') as file:
        file.write(f'''''')


    with driver.session() as session:
        session.execute_write(cl5.link_log_events, dataSet, graphviz_QueryLocationQ9)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step C10 -----------------------------------------------------------------------------------")


model_entities=cl4.model_entities

step_correlate_Events_to_Entities=True
if step_correlate_Events_to_Entities:
    stepName='StepC10 - Linking Events to Entities......'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q10"
    graphviz_QueryLocationQ10 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ10, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")

    for entity in model_entities:  # per entity

        # if entity[0] in include_entities:
        with driver.session() as session:
            session.execute_write(cl5.link_events_Entities, entity[0], entity[1], entity[2],entity[3], graphviz_QueryLocationQ10)
            print(f'\n     *{entity[3]+ str(entity[0])} E_EN relationships done')


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step C11 -----------------------------------------------------------------------------------")


eventAct_Rel=cl4.eventAct_Rel


step_linkingActivityClassToEvent=True
if step_linkingActivityClassToEvent:
    stepName='StepC11 - Linking Events to activities ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q11"
    graphviz_QueryLocationQ11 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ11, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("eventAct_Rel=", eventAct_Rel)
    print("...")
    print("")

    for item in eventAct_Rel:
        with driver.session() as session:
            session.execute_write(cl5.link_events_Activity, item[0], item[1], item[2], item[3], graphviz_QueryLocationQ11)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step C12 -----------------------------------------------------------------------------------")




step_linkingActivityPropertyClassToEvent=True
if step_linkingActivityPropertyClassToEvent:
    stepName='StepC11 - Linking Events to activities ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName="Q12"
    graphviz_QueryLocationQ12 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ12, 'w') as file:
        file.write(f'''''')


    print("")
    print("...")
    print("")


    with driver.session() as session:
        session.execute_write(cl5.link_events_ActivityProperty, graphviz_QueryLocationQ12)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


driver.close()

