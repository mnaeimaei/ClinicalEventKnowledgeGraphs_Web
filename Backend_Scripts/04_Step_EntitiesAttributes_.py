import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os

import B02_base as cl2
import D02_Step1_otherEntities_prepare1 as cl4
import D03_funcs2_otherEntities_importingNeo4J1 as cl5
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla




driver=cl4.driver
dataSet=cl4.EnP_dataSet
Perf_file_path = cl4.Perf_file_path
NodeEntity=cl4.NodeEntity



##################################################
userDirectory = f"../Data/registration/0_username.txt"
userPath = os.path.realpath(userDirectory)
with open(userPath, 'r') as file:
    for line in file:
        username = line
##################################################


medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '02_OtherEntities'
if not os.path.exists(outDir):
    os.mkdir(outDir)



selenium = cla.selenium


print(NodeEntity)
print(" ")
print("---------------------------------------- Step D1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    ############PART D ##########################################
    nodeTypesD = NodeEntity
    #['gender', 'newMorbids', 'age', 'treatedMorbids', 'untreatedMorbids', 'Disorder', 'Multimorbidity']
    relationTypesD = ["INCLUDED"]
    relationTypesD2 = [f'''INCLUDED {{Type:"last"}}''']

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


    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]


    nodeTypes = nodeTypesD + nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesD + relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.deleteRelation, relationTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.DeleteNodes, nodeTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2],graphviz_QueryLocationQ1)

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step D2 -----------------------------------------------------------------------------------")



step_Clear_OCT_Constraints=True
if step_Clear_OCT_Constraints:
    stepName='StepD2 - Dropping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.clearConstraint, None, driver, nodeTypes,graphviz_QueryLocationQ2)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D3 -----------------------------------------------------------------------------------")


step_createConstraint=True
if step_createConstraint:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')


    with driver.session() as session:
        session.execute_write(cl5.createConstraint, NodeEntity,graphviz_QueryLocationQ3)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D4 -----------------------------------------------------------------------------------")

NodeList=cl4.NodeList


step_createProperty=True
if step_createProperty:
    stepName='StepD4 - Creating Other Entities Node...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("NodeList=",NodeList)
    print("")

    for item in NodeList:
        with driver.session() as session:
            session.execute_write(cl5.OtherEntities_Nodes, item[0], item[1], item[2], item[3], item[4],graphviz_QueryLocationQ4)

    print("NodeEntity=", NodeEntity)



    # table to measure performance
    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################





driver.close()