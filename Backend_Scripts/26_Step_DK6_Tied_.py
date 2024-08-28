import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase

import A6_Scenario_Final_Step as ClN2
import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import O02_Step31_DK6_Tied_prepare as cl2b
import O03_funcs32_DK6_Tied_importingNeo4J as cl3f
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
outDir = downD + "/" +  '13_DK6'
if not os.path.exists(outDir):
    os.mkdir(outDir)




selenium = cla.selenium


print(" ")
print("---------------------------------------- Step N1 -----------------------------------------------------------------------------------")

EntityLists=ClN2.entityList
print(EntityLists)



step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepN1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART DK ##########################################
    relationTypesDK = ["TIED", "TYPE_OF"]
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
        session.execute_write(cl3f.deletePartRel, graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl3f.deletePartNode,EntityLists[i], graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl3f.deleteProperty,EntityLists[i], graphviz_QueryLocationQ1)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step N2 -----------------------------------------------------------------------------------")


start = time.time()
DFG=cl2b.DFG


list0=['DFG1',	'DFG1_featureValue',	'DFG2',	'DFG2_featureValue',	'DFG3',	'DFG3_feature',	'DFG3_featureValue',	'DFG4',	'DFG4_feature',	'DFG4_featureValue',	'DFG5',	'DFG5_feature',	'DFG5_featureValue']
list1=['DFG1_Domain',	'DFG1_featureValue_Domain',	'DFG2_Domain',	'DFG2_featureValue_Domain',	'DFG3_Domain',	'DFG3_feature_Domain',	'DFG3_featureValue_Domain','DFG4_Domain',	'DFG4_feature_Domain',	'DFG4_featureValue_Domain',	'DFG5_Domain',	'DFG5_feature_Domain',	'DFG5_featureValue_Domain']
list2=['DFG1_DomainConcept',	'DFG1_featureValue_DomainConcept',	'DFG2_DomainConcept',	'DFG2_featureValue_DomainConcept',	'DFG3_DomainConcept',	'DFG3_feature_DomainConcept',	'DFG3_featureValue_DomainConcept',	'DFG4_DomainConcept',	'DFG4_feature_DomainConcept',	'DFG4_featureValue_DomainConcept',	'DFG5_DomainConcept',	'DFG5_feature_DomainConcept',	'DFG5_featureValue_DomainConcept']
list3=['DFG1_DomainConceptLevel',	'DFG1_featureValue_DomainConceptLevel',	'DFG2_DomainConceptLevel',	'DFG2_featureValue_DomainConceptLevel',	'DFG3_DomainConceptLevel',	'DFG3_feature_DomainConceptLevel',	'DFG3_featureValue_DomainConceptLevel',	'DFG4_DomainConceptLevel',	'DFG4_feature_DomainConceptLevel',	'DFG4_featureValue_DomainConceptLevel',	'DFG5_DomainConceptLevel',	'DFG5_feature_DomainConceptLevel',	'DFG5_featureValue_DomainConceptLevel']

fileName = "Q2"
graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
with open(graphviz_QueryLocationQ2, 'w') as file:
    file.write(f'''''')

if DFG in list0:
    stepName = "This scenario doesn't use DK6"
    print('                      ')
    print(stepName)
    with open(graphviz_QueryLocationQ2, 'a') as file:
        file.write(f'''\n//This scenario doesn't use DK6\n\n''')



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

if DFG in list1:
    stepName = 'StepN2 - Creating Relationship between Activities and Domain ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc1_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_1, item[0], item[1],item[2] , graphviz_QueryLocationQ2)


    stepName = 'StepN3 - Creating Relationship between Activities Properties and Domain ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc1_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_1_Proprty, item[0], item[1],item[2] , graphviz_QueryLocationQ2 )




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




if DFG in list2:
    stepName='StepN2 - Creating Relationship between Domain to Concepts  ....'
    print('                      ')
    print(stepName)
    Form_OCT_MappingRelation = cl2b.DK5_2_Rel
    for item in Form_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.Activity_OCPS, item[0], item[1], item[2] , graphviz_QueryLocationQ2)

    stepName = 'StepN3 - Creating Relationship between Activities and Concept ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc2_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_2, item[0], item[1],item[2],item[3], graphviz_QueryLocationQ2)


    stepName = 'StepN4 - Creating Relationship between Activities Properties and Concept ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc2_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_2_Proprty, item[0], item[1],item[2],item[3], graphviz_QueryLocationQ2)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

if DFG in list3:
    stepName='StepN2 - Creating Relationship between Domain to Concepts  ....'
    print('                      ')
    print(stepName)
    Form_OCT_MappingRelation = cl2b.DK5_2_Rel
    for item in Form_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.Activity_OCPS, item[0], item[1], item[2], graphviz_QueryLocationQ2)

    stepName = 'StepN3 - Creating Relationship between Activities and Concept ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc3_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_3, item[0], item[1],item[2],item[3], graphviz_QueryLocationQ2)

    stepName = 'StepN4 - Creating Relationship between Activities Properties and Concept ....'
    print('                      ')
    print(stepName)
    relList = cl2b.sc3_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(cl3f.Domain_Scenario_3_Proprty, item[0], item[1],item[2],item[3], graphviz_QueryLocationQ2)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print("-------------------------------------------------------------------------------------------------------------------------")



driver.close()



