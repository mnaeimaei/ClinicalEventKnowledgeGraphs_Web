import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os

import B02_base as cl2
import D10_Step1_enRel_prepare3 as cl4
import D11_funcs2_enRel_importingNeo4J1 as cl5
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
print("---------------------------------------- Step D4 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    ############PART D ##########################################

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
    relationTypes = relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.deleteRelation, relationTypes, graphviz_QueryLocationQ4)
        session.execute_write(cl5.DeleteNodes, nodeTypes, graphviz_QueryLocationQ4)
        session.execute_write(cl5.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ4)

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step D5 -----------------------------------------------------------------------------------")



step_Clear_OCT_Constraints=True
if step_Clear_OCT_Constraints:
    stepName='StepD2 - Dropping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q5"
    graphviz_QueryLocationQ5 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ5, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.clearConstraint, None, driver, nodeTypes,graphviz_QueryLocationQ5)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step D6 -----------------------------------------------------------------------------------")


Treated=cl4.Treated


step_treated=True
if step_treated:
    stepName='StepJ2 - Adding Disorder to Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q6"
    graphviz_QueryLocationQ6 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ6, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("Treated=",Treated)
    print("")

    for item in Treated:
        with driver.session() as session:
            session.execute_write(cl5.admTreated_Fun, item[0], item[1], graphviz_QueryLocationQ6)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D7 -----------------------------------------------------------------------------------")


NotTreated=cl4.NotTreated


step_NotTreated=True
if step_NotTreated:
    stepName='StepJ2 - Adding Disorder to Not Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q7"
    graphviz_QueryLocationQ7 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ7, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("NotTreated=",NotTreated)
    print("")

    for item in NotTreated:
        with driver.session() as session:
            session.execute_write(cl5.admNotTreated_Fun, item[0], item[1], graphviz_QueryLocationQ7)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D8 -----------------------------------------------------------------------------------")


New=cl4.New


step_New=True
if step_New:
    stepName='StepJ2 - Adding Disorder to new Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()
    print("")
    print("Inputs:")
    print("New=",New)
    print("")

    fileName = "Q8"
    graphviz_QueryLocationQ8 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ8, 'w') as file:
        file.write(f'''''')

    for item in New:
        with driver.session() as session:
            session.execute_write(cl5.admNew_Fun, item[0], item[1], graphviz_QueryLocationQ8)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D9 -----------------------------------------------------------------------------------")


multiMorbidityValue=cl4.multiMorbidityValue


step_multiMorbidityValue=True
if step_multiMorbidityValue:
    stepName='StepJ5 - Adding Values to Multimorbidity ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q9"
    graphviz_QueryLocationQ9 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ9, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("multiMorbidityValue=",multiMorbidityValue)
    print("")

    for item in multiMorbidityValue:
        with driver.session() as session:
            session.execute_write(cl5.admMulti_Value, item[0], item[1], graphviz_QueryLocationQ9)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step D10 -----------------------------------------------------------------------------------")


treatedValue=cl4.treatedValue


step_treatedValue=True
if step_treatedValue:
    stepName='StepJ6 - Adding Values  to treatedMorbids  ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q10"
    graphviz_QueryLocationQ10 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ10, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("treatedValue=",treatedValue)
    print("")

    for item in treatedValue:
        with driver.session() as session:
            session.execute_write(cl5.admTreated_Value, item[0], item[1], graphviz_QueryLocationQ10)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D11 -----------------------------------------------------------------------------------")


notTreatedValue=cl4.notTreatedValue


step_notTreatedValue=True
if step_notTreatedValue:
    stepName='StepJ7 - Adding Values to untreatedMorbids'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q11"
    graphviz_QueryLocationQ11 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ11, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("notTreatedValue=",notTreatedValue)
    print("")

    for item in notTreatedValue:
        with driver.session() as session:
            session.execute_write(cl5.admNotTreated_Value, item[0], item[1], graphviz_QueryLocationQ11)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D12 -----------------------------------------------------------------------------------")


newValue=cl4.newValue


step_newValue=True
if step_newValue:
    stepName='StepJ8 - Adding Values to newMorbids ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q12"
    graphviz_QueryLocationQ12 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ12, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("newValue=",newValue)
    print("")

    for item in newValue:
        with driver.session() as session:
            session.execute_write(cl5.admNew_Value, item[0], item[1], graphviz_QueryLocationQ12)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print("-------------------------------------------------------------------------------------------------------------------------")





driver.close()