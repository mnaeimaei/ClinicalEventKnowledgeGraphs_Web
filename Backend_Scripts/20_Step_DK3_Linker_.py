import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase

import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import L02_Step25_DK3_Linker_prepare as cl2b
import L03_funcs26_DK3_Linker_importingNeo4J as cl3d
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = cl1.driver
OCTdataSet=cl1.OCTdataSet



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
outDir = downD + "/" +  '10_DK3'
if not os.path.exists(outDir):
    os.mkdir(outDir)



selenium = cla.selenium


print(" ")
print("---------------------------------------- Step K1 -----------------------------------------------------------------------------------")




step_Clear_DK1_DB=True
if step_Clear_DK1_DB:
    stepName='StepK1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()


    ############PART DK ##########################################
    relationTypesDK = ["LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')


    with driver.session() as session:
        session.execute_write(cl3d.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3d.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step K2 -----------------------------------------------------------------------------------")


DiagClinRel=cl2b.DiagClinRel



step_link_Entity1_Potential=True
if step_link_Entity1_Potential:
    stepName='StepK2 - Creating Relationship between Disorders and Clinical ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("DiagClinRel=",DiagClinRel)
    print("")


    for item in DiagClinRel:

        with driver.session() as session:
            session.execute_write(cl3d.Entity1_Potential_Entities, item[0], item[1], graphviz_QueryLocationQ2)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()


