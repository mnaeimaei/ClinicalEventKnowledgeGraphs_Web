import pandas as pd
import time, csv
from datetime import datetime
from neo4j import GraphDatabase
import os



import B02_base as cl2
import Q02_Step1_DFG_prepare_Scenario as cl4
import Q03_funcs_DFG_importingNeo4J_Scenario as cl5
import A1_Scenario_Step2 as Ne0
import A6_Scenario_Final_Step as Ne02
import B02_svg as svgFunction
import A5_EntryCol_Step3_Step as cla



driver=cl4.driver


Perf_file_path = cl4.Perf_file_path




#Temp





##################################################
userDirectory = f"../Data/registration/0_username.txt"
userPath = os.path.realpath(userDirectory)
with open(userPath, 'r') as file:
    for line in file:
        username = line
##################################################


medDownDfgDirectory = f'../../media/{username}/download/dfgTool'
downD = os.path.realpath(medDownDfgDirectory)
outDir = downD + "/" +  '15_Final'
if not os.path.exists(outDir):
    os.mkdir(outDir)







selenium = cla.selenium


print(" ")
print("---------------------------------------- Pre Step 1,2 -----------------------------------------------------------------------------------")



EntityLists=Ne02.entityList
print(EntityLists)

############PART D ##########################################
relationTypesD = [":REL", ":DF", ":DF_C", ":DF_E"]

relationTypes=relationTypesD



print(" ")
print("---------------------------------------- Step V1 -----------------------------------------------------------------------------------")



step_ClearDB=True
if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    stepName='StepV1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q1"
    graphviz_QueryLocationQ1 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ1, 'w') as file:
        file.write(f'''''')

    with driver.session() as session:
        session.execute_write(cl5.deleteRelation, relationTypes, graphviz_QueryLocationQ1)
        session.execute_write(cl5.deletePartRel, graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl5.deletePartNode,EntityLists[i], graphviz_QueryLocationQ1)
        for i in range(len(EntityLists)):
            session.execute_write(cl5.deleteProperty,EntityLists[i], graphviz_QueryLocationQ1)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)







print(" ")
print("---------------------------------------- Step V2 -----------------------------------------------------------------------------------")


model_entities=cl4.model_entities

step_createReifiedEntities=True
if step_createReifiedEntities:
    stepName='StepV2 - Modifying Entities Properties'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q2"
    graphviz_QueryLocationQ2 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ2, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")



    for relation in model_entities:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.modifyEntities, relation[0], relation[1], relation[2], graphviz_QueryLocationQ2)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V3 -----------------------------------------------------------------------------------")

model_relations=cl4.model_relations

step_createReifiedEntities=True
if step_createReifiedEntities:
    stepName='StepV3 - Creating Reified Entity Nodes'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q3"
    graphviz_QueryLocationQ3 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ3, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.createReifiedEntities, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ3)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V4 -----------------------------------------------------------------------------------")



step_entities_with_diff_ID_relationships=True
if step_entities_with_diff_ID_relationships:
    stepName='StepV4 - Creating Relationship between Entities with different ID...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q4"
    graphviz_QueryLocationQ4 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ4, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.entities_with_diff_ID_relationships, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ4)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V5 -----------------------------------------------------------------------------------")


step_RelatingReifiedEntitiesAndEntities=True
if step_RelatingReifiedEntitiesAndEntities:
    stepName='StepV5 - Relating Reified Entity Nodes to Non-Reified Entity Node'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q5"
    graphviz_QueryLocationQ5 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ5, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.RelatingReifiedEntitiesAndEntities, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ5)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V6 -----------------------------------------------------------------------------------")


step_correlate_ReifiedEntities_to_Event=True
if step_correlate_ReifiedEntities_to_Event:
    stepName='StepV6 - Correlate Reified Entities Nodes to Events Node'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q6"
    graphviz_QueryLocationQ6 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ6, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.correlate_ReifiedEntities_to_Event, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ6)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V7 -----------------------------------------------------------------------------------")

include_DF=cl4.include_DF


step_createDF=True
if step_createDF:
    stepName='StepV7 - Creating DF Relationship for Absolute Entities...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q7"
    graphviz_QueryLocationQ7 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ7, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")
    for entity in include_DF:  # per entity
        with driver.session() as session:
            session.execute_write(cl5.createDF, entity[0], entity[1], graphviz_QueryLocationQ7)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V8 -----------------------------------------------------------------------------------")


step_delete_Polluted_Reified_DF=True
if step_delete_Polluted_Reified_DF:
    stepName='StepV8 - Deleting Polluted DF  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q8"
    graphviz_QueryLocationQ8 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ8, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.deletePuluted_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ8)


    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V9 -----------------------------------------------------------------------------------")


step_delete_Polluted_Wrong_DF=True
if step_delete_Polluted_Wrong_DF:
    stepName='StepV9 - Deleting Wrong DF  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q9"
    graphviz_QueryLocationQ9 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ9, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.deleteWrong_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ9)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V10 -----------------------------------------------------------------------------------")


step_deleteExtra_Reified_DF=True
if step_deleteExtra_Reified_DF:
    stepName='StepV10 - Deleting Reverse Reified Relationship  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q10"
    graphviz_QueryLocationQ10 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ10, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(cl5.deleteExtra_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ10)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V11 -----------------------------------------------------------------------------------")



step_deletePolluted_CoRR_Reified_Events=True
if step_deletePolluted_CoRR_Reified_Events:
    stepName='StepV11 - Deleting correlation between event and Reified entities  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q11"
    graphviz_QueryLocationQ11 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ11, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(cl5.deletePolluted_CoRR_Reified_Events, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ11)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V12 -----------------------------------------------------------------------------------")



step_deletePolluted_CoRR_Reified_Events_part2=True
if step_deletePolluted_CoRR_Reified_Events_part2:
    stepName='StepV12 - Restoring correlation between event and Reified entities which wrongly deleted in step 12  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q12"
    graphviz_QueryLocationQ12 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ12, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(cl5.deletePolluted_CoRR_Reified_Events_2, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ12)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step V13 -----------------------------------------------------------------------------------")



step_wrong_rel_and_wrong_refied_entity=True
if step_wrong_rel_and_wrong_refied_entity:
    stepName='StepV13 - Deleting _wrong_rel_and_wrong_refied_entity  ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q13"
    graphviz_QueryLocationQ13 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ13, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:
        with driver.session() as session:
            session.execute_write(cl5.wrong_reified, relation[0], relation[1], relation[2], relation[3], relation[4], graphviz_QueryLocationQ13)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step V14 -----------------------------------------------------------------------------------")

include_DF=cl4.include_DF


step_aggregateDF_Absolute=True
if step_aggregateDF_Absolute:
    stepName='StepV14 - Aggregating Absolute ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q14"
    graphviz_QueryLocationQ14 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ14, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")

    for entity in include_DF:
        with driver.session() as session:
            session.execute_write(cl5.aggregateDF_Absolute, entity[0], entity[1], graphviz_QueryLocationQ14)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V15 -----------------------------------------------------------------------------------")

Final_AG_New=cl4.Final_AG_New


step_aggregateDF_Relative=True
if step_aggregateDF_Relative:
    stepName='StepV15 - Aggregating Relatively...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q15"
    graphviz_QueryLocationQ15 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ15, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("Final_AG_New=",Final_AG_New)
    print("")

    En1 = Final_AG_New[0]
    En2 = Final_AG_New[1]
    ID_Value_List = Final_AG_New[2]
    print(En1)
    print(En2)
    print(ID_Value_List)
    for i in range(len(ID_Value_List)):
        eID = ID_Value_List[i]
        print(eID)
        with driver.session() as session:
            session.execute_write(cl5.aggregateDF_Relative, En1, En2, eID, graphviz_QueryLocationQ15)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V16 -----------------------------------------------------------------------------------")

status=cl4.status
Final_AG_All=cl4.Final_AG_All
Final_AG_All_ID=cl4.Final_AG_All_ID

step_aggregateDF_All=True
if step_aggregateDF_All:
    stepName='StepV16 - Aggregating All...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q16"
    graphviz_QueryLocationQ16 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ16, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("status=",status)
    print("Final_AG_All=",Final_AG_All)
    print("Final_AG_All_ID=",Final_AG_All_ID)
    print("")

    if status==11:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_All, entity, entity[0], graphviz_QueryLocationQ16)


    if status==22:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_All, entity, entity[0], graphviz_QueryLocationQ16)


    if status==33:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_All_inactiveID, entity, entity[0], graphviz_QueryLocationQ16)



    if status==44:
        for entity, ID in zip(Final_AG_All, Final_AG_All_ID):
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_All_activeID, entity, entity[0], ID, graphviz_QueryLocationQ16)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V17 -----------------------------------------------------------------------------------")


nodeReal=cl4.nodeReal

step_relEntity=True
if step_relEntity:
    stepName='StepV17 - Entities Relations... (DFG 6)'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q17"
    graphviz_QueryLocationQ17 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ17, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("nodeReal=",nodeReal)
    print("")
    for relation in nodeReal:  # per relation
        print(relation)
        for item in relation:
            print(item)
            item0ID = item[0][0]
            item0 = item[1][0]
            item1 = item[1][-1]
            item1ID1 = item[2][0]
            item1ID2 = item[2][1]
            with driver.session() as session:
                session.execute_write(cl5.relEntity, item0ID, item0, item1, item1ID1,item1ID2, graphviz_QueryLocationQ17)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V18 -----------------------------------------------------------------------------------")


nodeReal=cl4.nodeReal

step_relEntityLower=True
if step_relEntityLower:
    stepName='StepV18 - Entities Relations lower... (DFG 6)'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q18"
    graphviz_QueryLocationQ18 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ18, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("nodeReal=",nodeReal)
    print("")
    for relation in nodeReal:  # per relation
        print(relation)
        for item in relation:
            print(item)
            item1 = item[1][-1]
            item3 = item[3][0]
            item4 = item[4][0]
            item0ID = item[0][0]
            item0 = item[1][0]
            id1=item[2][0]
            id2 = item[2][1]
            with driver.session() as session:
                session.execute_write(cl5.relEntityLower, item1, item3,item4,item0,item0ID, id1, id2, graphviz_QueryLocationQ18)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V19 -----------------------------------------------------------------------------------")



step_DF_Propery=True
if step_DF_Propery:
    stepName='StepV19 - DF for properties... (DFG Property)'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q19"
    graphviz_QueryLocationQ19 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ19, 'w') as file:
        file.write(f'''''')


    print("")
    with driver.session() as session:
        session.execute_write(cl5.DF_Propery, graphviz_QueryLocationQ19)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V20 -----------------------------------------------------------------------------------")


include_DF=cl4.include_DF


step_aggregateDF_Absolute_property=True
if step_aggregateDF_Absolute_property:
    stepName='StepV20 - Aggregating Absolute for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q20"
    graphviz_QueryLocationQ20 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ20, 'w') as file:
        file.write(f'''''')

    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")

    for entity in include_DF:
        with driver.session() as session:
            session.execute_write(cl5.aggregateDF_AbsoluteProperty, entity[0], entity[1], graphviz_QueryLocationQ20)
    with driver.session() as session:
        session.execute_write(cl5.DF_AbsolutePropery, graphviz_QueryLocationQ20)



    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V21 -----------------------------------------------------------------------------------")


Final_AG_New=cl4.Final_AG_New


step_aggregateDF_Relative_property=True
if step_aggregateDF_Relative_property:
    stepName='StepV20 - Aggregating Relative for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q21"
    graphviz_QueryLocationQ21 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ21, 'w') as file:
        file.write(f'''''')


    print("")
    print("Inputs:")
    print("Final_AG_New=",Final_AG_New)
    print("")

    En1 = Final_AG_New[0]
    En2 = Final_AG_New[1]
    ID_Value_List = Final_AG_New[2]
    print(En1)
    print(En2)
    print(ID_Value_List)
    for i in range(len(ID_Value_List)):
        eID = ID_Value_List[i]
        print(eID)
        with driver.session() as session:
            session.execute_write(cl5.aggregateDF_RelativeProperty, En1, En2, eID, graphviz_QueryLocationQ21)

    with driver.session() as session:
        session.execute_write(cl5.DF_RelativePropery, graphviz_QueryLocationQ21)




    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V22 -----------------------------------------------------------------------------------")


status=cl4.status
Final_AG_All=cl4.Final_AG_All
Final_AG_All_ID=cl4.Final_AG_All_ID


step_aggregateDF_All_property=True
if step_aggregateDF_All_property:
    stepName='StepV22 - Aggregating All for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()

    fileName = "Q22"
    graphviz_QueryLocationQ22 = outDir + "/" + fileName + ".txt"
    with open(graphviz_QueryLocationQ22, 'w') as file:
        file.write(f'''''')


    if status==11:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_AllProperty, entity, entity[0], graphviz_QueryLocationQ22)


    if status==22:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_AllProperty, entity, entity[0], graphviz_QueryLocationQ22)


    if status==33:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_AllProperty_inactiveID, entity, entity[0], graphviz_QueryLocationQ22)


    if status==44:
        for entity, ID in zip(Final_AG_All, Final_AG_All_ID):
            with driver.session() as session:
                session.execute_write(cl5.aggregateDF_AllProperty_activeID, entity, entity[0], ID, graphviz_QueryLocationQ22)

    with driver.session() as session:
        session.execute_write(cl5.DF_AllPropery, graphviz_QueryLocationQ22)





    end = time.time()
    cl2.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step Not Used 1 -----------------------------------------------------------------------------------")


driver.close()