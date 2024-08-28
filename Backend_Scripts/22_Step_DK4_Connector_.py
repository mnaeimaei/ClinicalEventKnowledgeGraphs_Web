import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase

import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import M02_Step27_DK4_Connector_prepare as cl2b
import M03_funcs28_DK4_Connector_importingNeo4J as cl3e
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
outDir = downD + "/" +  '11_DK4'
if not os.path.exists(outDir):
    os.mkdir(outDir)




selenium = cla.selenium

print(" ")
print("---------------------------------------- Step L1 -----------------------------------------------------------------------------------")



step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepL1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART DK ##########################################
    relationTypesDK = ["CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3e.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3e.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step L2 -----------------------------------------------------------------------------------")

icdOCT=cl2b.icdOCT

step_link_potential_OCPS=True
if step_link_potential_OCPS:
    stepName='StepL2 - Creating Relationship between Clinical and Concepts ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')


    for item in icdOCT:
        with driver.session() as session:
            session.execute_write(cl3e.Potential_OCPS, item[0], item[1], graphviz_QueryLocationQ2)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)







print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()



