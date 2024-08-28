import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os

import B02_base as cl2
import D06_Step1_enRel_prepare2 as cl4
import D07_funcs2_enRel_importingNeo4J1 as cl5
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla






driver=cl4.driver
dataSet=cl4.EnP_dataSet
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
outDir = downD + "/" +  '02_EntityRel'
if not os.path.exists(outDir):
    os.mkdir(outDir)




selenium = cla.selenium

print(" ")
print("---------------------------------------- Step D1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    ############PART D ##########################################

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


    nodeTypes = nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesD + relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2



    with driver.session() as session:
        session.execute_write(cl5.deleteRelation, relationTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.DeleteNodes, nodeTypes,graphviz_QueryLocationQ1)
        session.execute_write(cl5.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D2 -----------------------------------------------------------------------------------")

print(nodeTypes)

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
        session.execute_write(cl5.clearConstraint, None, driver, nodeTypes, graphviz_QueryLocationQ2)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step D5 -----------------------------------------------------------------------------------")


RelFinal=cl4.RelFinal


step_createDomains=True
if step_createDomains:
    stepName='StepD5 - Creating Rel between Entities ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("RelFinal=",RelFinal)
    print("")

    for item in RelFinal:
        with driver.session() as session:
            session.execute_write(cl5.intraEntitiesRel,  item[0], item[1], item[2], item[3], graphviz_QueryLocationQ3)





    # table to measure performance
    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################






driver.close()