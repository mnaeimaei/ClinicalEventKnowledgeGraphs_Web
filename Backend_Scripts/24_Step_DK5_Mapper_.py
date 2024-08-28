import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase

import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import N02_Step29_DK5_Mapper_prepare as cl2b
import N03_funcs30_DK5_Mapper_importingNeo4J as cl3f
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla

from tqdm import tqdm


print("************************** From cl1: ****************************************************************************")


driver = cl1.driver




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
outDir = downD + "/" +  '12_DK5'
if not os.path.exists(outDir):
    os.mkdir(outDir)



selenium = cla.selenium

print(" ")
print("---------------------------------------- Step M1 -----------------------------------------------------------------------------------")




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepM1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()


    ############PART DK ##########################################
    relationTypesDK = ["MAPPED_TO", "TIED","TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV


    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')


    with driver.session() as session:
        session.execute_write(cl3f.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3f.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step M2 -----------------------------------------------------------------------------------")

Activity_OCT_MappingRelation=cl2b.final_Activity_OCT_MappingRelation


step_link_Activity_OCPS=True
if step_link_Activity_OCPS:
    stepName='StepM2 - Creating Relationship between Activity and Concepts  ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    for item in Activity_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.Activity_OCPS, item[0], item[1], item[2], item[3], graphviz_QueryLocationQ2)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print("-------------------------------------------------------------------------------------------------------------------------")

print(" ")
print("---------------------------------------- Step M3 -----------------------------------------------------------------------------------")

Activity_OCT_MappingRelation=cl2b.final_Activity_OCT_MappingRelation


step_link_ActivityProperty_OCPS=True
if step_link_ActivityProperty_OCPS:
    stepName='StepM2 - Creating Relationship between Activity Property and Concepts  ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')

    for item in Activity_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.ActivityProperty_OCPS, item[0], item[1], item[2], item[3], graphviz_QueryLocationQ3)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")

driver.close()



