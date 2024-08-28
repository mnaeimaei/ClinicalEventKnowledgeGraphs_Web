


import time


import A5_EntryCol_Step3_Step as cl1
import B02_base as cl2
import I06_Step19_OCTrel_Prepare as cl2b
import I07_funcs20_OCTrel_importingNeo4J as cl3c



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
    step_link_Concepts = variables.get('step_link_Concepts', None)
    step_link_Concepts_for_loop = variables.get('step_link_Concepts_for_loop', None)
    step_modify_concepts = variables.get('step_modify_concepts', None)


else:
    step_Clear_OCT_DB = 1
    step_link_Concepts = 1
    step_link_Concepts_for_loop = 1
    step_modify_concepts = 1



print("step_Clear_OCT_DB =", step_Clear_OCT_DB)
print("step_link_Concepts =", step_link_Concepts)
print("step_link_Concepts_for_loop =", step_link_Concepts_for_loop)
print("step_modify_concepts =", step_modify_concepts)





extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
with open(extDirectory, 'w') as file:
    file.write(f"step_Clear_OCT_DB={step_Clear_OCT_DB}\n")
    file.write(f"step_link_Concepts={step_link_Concepts}\n")
    file.write(f"step_link_Concepts_for_loop={step_link_Concepts_for_loop}\n")
    file.write(f"step_modify_concepts={step_modify_concepts}\n")



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
print("step_link_Concepts =", step_link_Concepts)
print("step_link_Concepts_for_loop =", step_link_Concepts_for_loop)
print("step_modify_concepts =", step_modify_concepts)

print("************************** DfgTool: ****************************************************************************")


medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '07_OCT_REL'
if not os.path.exists(outDir):
    os.mkdir(outDir)


print("************************** From cl1: ****************************************************************************")


driver = cl1.driver
OCTdataSet=cl1.OCTdataSet


Perf_file_path = cl2b.Perf_file_path


print(" ")
print("---------------------------------------- Step H1 -----------------------------------------------------------------------------------")



if step_Clear_OCT_DB==1:
    stepName='StepH1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART I ##########################################

    relationTypesI = ["ANCESTOR_OF"]

    ############PART DK2 ##########################################

    relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]


    relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3c.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl3c.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2], graphviz_QueryLocationQ1)


    modify_config("step_Clear_OCT_DB", "0")

    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step H5 -----------------------------------------------------------------------------------")


if step_link_Concepts==1:
    stepName='StepH5 - Creating Relationship between all Concepts ....'
    print('                      ')
    print(stepName)

    OCPS_REL_MappingRelation = cl2b.OCT_REL_MappingRelation
    #print("OCPS_REL_MappingRelation=", OCPS_REL_MappingRelation)
    length = len(OCPS_REL_MappingRelation)

    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    start_index = step_link_Concepts_for_loop


    if start_index < len(OCPS_REL_MappingRelation):
        for item, k in zip(OCPS_REL_MappingRelation[start_index:], range(start_index, len(OCPS_REL_MappingRelation))):
            #print(k)
            with driver.session() as session:
                session.execute_write(cl3c.link_Concepts, item[0], item[1], item[2], item[3], graphviz_QueryLocationQ2)
            formatted_number = "{:.2f}".format(100 * k / len(OCPS_REL_MappingRelation))
            modify_config("step_link_Concepts_for_loop", k)
            print("Completed: ", formatted_number, "%")

    modify_config("step_link_Concepts", "0")




    # Shorten the Q2
    with open(graphviz_QueryLocationQ2, 'r') as file:
        lines = file.readlines()
    first_50_lines = lines[:50]
    graphviz_QueryLocationQ2_short = outDir + "/Q2_short.txt"
    with open(graphviz_QueryLocationQ2_short, 'w') as new_file:
        new_file.writelines(first_50_lines)
    #Finish Shorten the Q2



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H6 -----------------------------------------------------------------------------------")


if step_modify_concepts==1:
    stepName='StepH6 - Modifying Concept ....'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl3c.modify_concept, graphviz_QueryLocationQ3)


    modify_config("step_modify_concepts", "0")


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)






print("-------------------------------------------------------------------------------------------------------------------------")




driver.close()

extDirectory = f"../Data/users/{username}/0_utilsExecution/taskExecution.txt"
with open(extDirectory, 'w') as file:
 pass