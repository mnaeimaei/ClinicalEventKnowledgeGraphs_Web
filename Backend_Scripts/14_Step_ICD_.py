import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase

import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import H02_Step17_ClinicalEntity_Prepare as cl2b
import H03_funcs18_ClinicalEntity_importingNeo4J as cl3b
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = cl1.driver
CEdataSet=cl1.CEdataSet


Perf_file_path = cl2b.Perf_file_path



##################################################
userDirectory = f"../Data/registration/0_username.txt"
userPath = os.path.realpath(userDirectory)
with open(userPath, 'r') as file:
    for line in file:
        username = line
##################################################


medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '05_ICD'
if not os.path.exists(outDir):
    os.mkdir(outDir)




selenium = cla.selenium



print(" ")
print("---------------------------------------- Step G1 -----------------------------------------------------------------------------------")




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepG1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()




    ############PART H ##########################################
    nodeTypesH = ["Clinical"]

    ############PART I ##########################################
    nodeTypesI = ["Concept"]
    relationTypesI = ["ANCESTOR_OF"]

    ############PART DK2 ##########################################

    relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    nodeTypes = nodeTypesH + nodeTypesI
    relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3b.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3b.DeleteNodes, nodeTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3b.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step G2 -----------------------------------------------------------------------------------")


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
        session.execute_write(cl3b.clearConstraint, None, driver, nodeTypes, graphviz_QueryLocationQ2)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step G3 -----------------------------------------------------------------------------------")




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
        session.execute_write(cl3b.createConstraint, nodeTypesH, graphviz_QueryLocationQ3)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step G4 -----------------------------------------------------------------------------------")



caseICD=cl2b.caseICD

step_icd_Nodes=True
if step_icd_Nodes :
    stepName='StepF4 - Creating ICD Nodes....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')

    for item in caseICD:
        with driver.session() as session:
            session.execute_write(cl3b.icd_Nodes, item[0], item[1], item[2], item[3], graphviz_QueryLocationQ4)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- -----------------------------------------------------------------------------------")


driver.close()