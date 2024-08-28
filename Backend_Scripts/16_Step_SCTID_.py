
import time


import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import I02_Step19_OCT_Prepare as cl2b
import I03_funcs20_OCT_importingNeo4J as cl3c


import os
print("************************** import user name: ****************************************************************************")




userDirectory = f"../Data/registration/0_username.txt"
userPath = os.path.realpath(userDirectory)
with open(userPath, 'r') as file:
    for line in file:
        username = line



print("************************** Utils Execution: ****************************************************************************")




def load_config(username):
    variables = {}
    extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
    extPath = os.path.realpath(extDirectory)
    with open(extPath, 'r') as file:
        if file.read(1):
            # If not EOF, go back to the start of the file
            file.seek(0)
            for line in file:
                line = line.strip()
                exec(line, {}, variables)
    return variables

variables=load_config(username)



if bool(variables):

    step_Clear_OCT_DB = variables.get('step_Clear_OCT_DB', None)
    step_Clear_OCT_Constraints = variables.get('step_Clear_OCT_Constraints', None)
    step_createConstraint = variables.get('step_createConstraint', None)
    step_load_OCPS_Concepts = variables.get('step_load_OCPS_Concepts', None)
    step_load_OCPS_Concepts_for_loop = variables.get('step_load_OCPS_Concepts_for_loop', None)
else:
    step_Clear_OCT_DB = 1
    step_Clear_OCT_Constraints = 1
    step_createConstraint = 1
    step_load_OCPS_Concepts = 1
    step_load_OCPS_Concepts_for_loop = 0


print("step_Clear_OCT_DB =", step_Clear_OCT_DB)
print("step_Clear_OCT_Constraints =", step_Clear_OCT_Constraints)
print("step_createConstraint =", step_createConstraint)
print("step_load_OCPS_Concepts =", step_load_OCPS_Concepts)
print("step_load_OCPS_Concepts_for_loop =", step_load_OCPS_Concepts_for_loop)





extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
with open(extDirectory, 'w') as file:
    file.write(f"step_Clear_OCT_DB={step_Clear_OCT_DB}\n")
    file.write(f"step_Clear_OCT_Constraints={step_Clear_OCT_Constraints}\n")
    file.write(f"step_createConstraint={step_createConstraint}\n")
    file.write(f"step_load_OCPS_Concepts={step_load_OCPS_Concepts}\n")
    file.write(f"step_load_OCPS_Concepts_for_loop={step_load_OCPS_Concepts_for_loop}\n")




def modify_config(config_key, new_value):
    extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
    file_path = os.path.realpath(extDirectory)
    with open(file_path, 'r') as file:
        lines = file.readlines()
    with open(file_path, 'w') as file:
        for line in lines:
            if line.startswith(config_key + '='):
                # Change the value to 2
                line = f'{config_key}={new_value}\n'
            file.write(line)





print("step_Clear_OCT_DB =", step_Clear_OCT_DB)
print("step_Clear_OCT_Constraints =", step_Clear_OCT_Constraints)
print("step_createConstraint =", step_createConstraint)
print("step_load_OCPS_Concepts =", step_load_OCPS_Concepts)
print("step_load_OCPS_Concepts_for_loop =", step_load_OCPS_Concepts_for_loop)



print("************************** DfgTool: ****************************************************************************")



medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '06_OCT_Node'
if not os.path.exists(outDir):
    os.mkdir(outDir)

print("************************** From cl1: ****************************************************************************")


driver = cl1.driver
OCTdataSet=cl1.OCTdataSet


Perf_file_path = cl2b.Perf_file_path


print(" ")
print("---------------------------------------- Step H1 -----------------------------------------------------------------------------------")

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

nodeTypes = nodeTypesI
relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2


if step_Clear_OCT_DB==1:
    stepName='StepH1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()


    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3c.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3c.DeleteNodes, nodeTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3c.DeleteAllNodesRels, nodeTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3c.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)

    modify_config("step_Clear_OCT_DB", "0")

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H2 -----------------------------------------------------------------------------------")



if step_Clear_OCT_Constraints==1:
    stepName='StepD2 - Dropping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3c.clearConstraint, None, driver, nodeTypes, graphviz_QueryLocationQ2)


    modify_config("step_Clear_OCT_Constraints", "0")

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H3 -----------------------------------------------------------------------------------")




if step_createConstraint==1:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3c.createConstraint, nodeTypesI, graphviz_QueryLocationQ3)



    modify_config("step_createConstraint", "0")

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


'''
print(" ")
print("---------------------------------------- Step H4 Old -----------------------------------------------------------------------------------")

header_OCT_Node=cl2b.header_OCT_Node
OCT_Neo4JImport_OCT_Node_FileName=cl2b.OCT_Neo4JImport_OCT_Node_FileName

step_load_OCPS_Concepts=True
if step_load_OCPS_Concepts :
    stepName='StepH4 - Creating OCPS Concepts Nodes ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')


    cl3c.loadOCPSConcepts(driver, graphviz_QueryLocationQ4, header_OCT_Node, OCT_Neo4JImport_OCT_Node_FileName, OCTdataSet)  # generate query to create all events with all log columns as properties



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

'''



print(" ")
print("---------------------------------------- Step H4 New -----------------------------------------------------------------------------------")





if step_load_OCPS_Concepts==1 :
    stepName='StepH4 - Creating OCPS Concepts Nodes ....'
    print('                      ')
    print(stepName)
    octNodeList = cl2b.octNodeList
    #print(len(octNodeList))
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')

    start_index = step_load_OCPS_Concepts_for_loop

    if start_index < len(octNodeList):
        for item, k in zip(octNodeList[start_index:], range(start_index, len(octNodeList))):
            #print(k)
            with driver.session() as session:
                session.execute_write(cl3c.loadOCPSConceptsNew, item[0], item[1], item[2], item[3], item[4], item[5],
                                      item[6], item[7], graphviz_QueryLocationQ4)
            formatted_number = "{:.2f}".format(100 * k / len(octNodeList))
            modify_config("step_load_OCPS_Concepts_for_loop", k)
            print("Completed: ", formatted_number, "%")

    modify_config("step_load_OCPS_Concepts", "0")

    # Shorten the Q4
    with open(graphviz_QueryLocationQ2, 'r') as file:
        lines = file.readlines()
    first_50_lines = lines[:50]
    graphviz_QueryLocationQ2_short = outDir + "/Q4_short.txt"
    with open(graphviz_QueryLocationQ2_short, 'w') as new_file:
        new_file.writelines(first_50_lines)
    #Finish Shorten the Q4

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")





driver.close()



extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
with open(extDirectory, 'w') as file:
 pass