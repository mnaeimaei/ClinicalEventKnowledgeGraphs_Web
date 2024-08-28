import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase
import A1_Scenario_Step2 as clN
import A5_EntryCol_Step3_Step as cl1
import A6_Scenario_Final_Step as ClN2
import B02_base as cl2
import P02_Step33_DK7_Bond_prepare as cl2b
import P03_funcs34_DK7_Bond_importingNeo4J as cl3g
import A1_Scenario_Step2 as clo
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
outDir = downD + "/" +  '14_DK7'
if not os.path.exists(outDir):
    os.mkdir(outDir)



selenium = cla.selenium


print(" ")
print("---------------------------------------- Step O1 -----------------------------------------------------------------------------------")

EntityLists=ClN2.entityList
print(EntityLists)


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepO1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()




    ############PART DK ##########################################
    relTypePartially = ["CORR","Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesV


    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3g.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3g.deletePartiallyRel, relTypePartially[0],relTypePartially[1],relTypePartially[2], graphviz_QueryLocationQ1)
        session.execute_write(cl3g.deletePartRel, graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl3g.deletePartNode,EntityLists[i], graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl3g.deleteProperty,EntityLists[i], graphviz_QueryLocationQ1)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step O2 -----------------------------------------------------------------------------------")

stepName = 'StepO3 - Creating Relationship between Events and Disorders ....'
print('                      ')
print(stepName)
start = time.time()
myInput=clo.myInput
print("myInput=",myInput)

fileName = "Q2"
graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
with open(graphviz_QueryLocationQ2, 'w') as file:
    file.write(f'''''')


if myInput == 'main_Entities': #1
    print("This scenario doesn't use DK7")
    with open(graphviz_QueryLocationQ2, 'a') as file:
        file.write(f'''\n//This scenario doesn't use DK7\n\n''')



if myInput == 'main_Entities_plus_Disorder': #2
    relList = cl2b.sc2_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_1, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_ICD': #3
    relList = cl2b.sc3_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_2, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_ICD_level_doesnt_work': #4
    relList = cl2b.sc4_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_3, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_SCT': #5
    relList = cl2b.sc5_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_4, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_SCT_level': #6
    relList = cl2b.sc6_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_5, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_SCT_Level_One': #7
    relList = cl2b.sc7_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_6, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_ICD_one': #8
    relList = cl2b.sc8_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_7, item[0], item[1],"2", graphviz_QueryLocationQ2)



if myInput == 'main_Entities_plus_SCT_one': #9
    relList = cl2b.sc9_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Scenario_8, item[0], item[1],"2", graphviz_QueryLocationQ2)




end = time.time()
cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()



