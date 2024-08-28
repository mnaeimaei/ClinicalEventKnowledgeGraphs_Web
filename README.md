# CEKG
A Tool for Constructing Event Graphs of the Care Pathways of Multi-Morbid Patients.

# 1-The Tool Address
First, go to [CEKG_Web](https://cekg-db1cc0d27386.herokuapp.com/) to see the first page of the tool.


# 2-The Home Page
Click on “START CEKG” to begin using the app.

# 3-Login Page
The username and password for the demo program committee are in the Appendix of the paper.


# 4-The Profile Page
On the next page, you can see the directory and output of the files you have created using the app. These files will be shown if you have previously used the app. Whether it's your first time using the app or you've used it before, you can click on "START CEKG" to begin building a new CEKG with the app.


# 5-The Care pathways options
On the next page, you need to select the type of care pathway you want to create.


## Step1) Select the type of care pathway you want to create.


## Step2) Other Configuration of the care pathways

### If you select the first or second option from Step 1, you will have 8 options to choose from regarding the details about activities you want to include in the care pathways.
### If you select the third, fourth, or fifth option from Step 1, you will have 12 options to choose from regarding the details about activities you want to include in the care pathways.

### If you select the sixth option from Step 1, you will have 2 options to decide whether you prefer to show only one of the following: Admission Disorders, Admission Treated Disorders, Admission Not Treated Disorders, or Admission New Disorders, or to display all of them together.

## Step3) Entities Configuration

### If you select the first, second, third, fourth, or fifth option from Step 1, you will have 9 options to choose from for the entities to be included in the care pathways.

### If you select the sixth option from Step 1, you will have 1 option for selecting the entities to be included in the care pathways.


## Step4) Activity Label


### If you select the first, second, third, fourth, or fifth option from Step 1, you will have 3 options for the activity labels to be included in the care pathways

### If you select the sixth option from Step 1, you will have 1 option for the activity label to be included in the care pathway.

# 6-Uploading the Dataset

Download the Test Dataset
The test dataset is provided as an Excel file (.xlsx). You can download it from this link: [DataSet](https://cekg-db1cc0d27386.herokuapp.com/](https://docs.google.com/spreadsheets/d/18PDQisLKwPPh6Gl7v5bCzYzUsiKmIGs_WYOzksPh5eM/edit?usp=sharing )

About the Test Dataset
The test dataset consists of several tabs within the Excel file. These tabs could also be individual CSV files, but to simplify the testing process, all the CSV files have been combined as tabs in a single Excel workbook. The names of the tabs can be customized as needed. These tabs are essential for creating the Clinical Event Knowledge Graph as presented in the paper [Clinical Event Knowledge Graphs: Enriching Healthcare Event Data with Entities and Clinical Concepts-Research Paper](https://link.springer.com/chapter/10.1007/978-3-031-56107-8_23)
You can also use your own dataset by considering this Excel file as a template.
Here, we will discuss what each tab in the workbook represents.

## Event Log Tab

This tab consists of our event log, which can be either a single-entity or multi-entity event log. Entities represent distinct existences. Sometimes, the terms “case notion,” “case,” “object,” and “dimensional” are used interchangeably. The term "multi-entity event log" is sometimes considered equivalent to “object-centric event log” or “multi-dimensional event log.” In the multi-entity event log definition, each entity is defined with its origin and IDs. The tab contains several columns:

- **Event_ID:** Contains the ID of each event.
- **Timestamp:** Contains the time and date of activities.
- **Activity:** Consists of the activity label of the event.
- **Activity_Synonym:** Contains abbreviations of activity labels. For example, BGT for Blood Gas Test.
- **Activity_Attributes_ID:** A unique foreign key ID for each distinct feature and value. For example:
  - `po2=295 → Activity_Properties_ID=1`
  - `lactate=3.23 → Activity_Properties_ID=2`
  - `Blood pressure=137/79 → Activity_Properties_ID=3`
  - `po2=412 → Activity_Properties_ID=4` (same feature but different value, so a different ID)
  - `lactate=0.73 → Activity_Properties_ID=5` (same feature but different value, so a different ID)
  - `po2=295 → Activity_Properties_ID=1` (same feature and same value, so the same ID)
  - `lactate=3.23 → Activity_Properties_ID=2` (same feature and same value, so the same ID)
- **Activity_Instance_ID:** A unique foreign key identifier for each distinct activity, considering its features and values. For example:
  - First event: `Blood Gas Test: po2=295, lactate=3.23 → Activity_Value_ID=1`
  - Second event: `BP_measurement: Blood pressure=137/79 → Activity_Value_ID=2` (different activity from the first event)
  - Third event: `Blood Gas Test: po2=412, lactate=0.73 → Activity_Value_ID=3` (same activity as the first event but with different feature values)
  - Fourth event: `Blood Gas Test: po2=295, lactate=3.23 → Activity_Value_ID=1` (same activity as the first event with the same feature values)
- **Entity1_origin** and **Entity1_ID:** Contain the origin and ID of each instance of the first entity. For example, the first entity instances could be “Patient1,” “Patient2,” etc.
- **Entity2_origin** and **Entity2_ID:** Contain the origin and ID of each instance of the second entity. For example, the second entity instances could be “Admission11,” “Admission12,” etc.


## EntitiesAttributes

This Excel tab contains the attributes of our entities. Each entity can have several attributes, which can either be used as entities themselves or only as attributes.

For example, age, gender, and admission are attributes of the Patient entity, as each patient has an age, gender, and admission sequence. Additionally, multimorbidity, treated multimorbidity, untreated multimorbidity, and new multimorbidity are attributes of the Admission entity. Similarly, each disorder is an attribute of multimorbidity, treated multimorbidity, untreated multimorbidity, and new multimorbidity.

- **Origin:** This column shows the type of attribute.
- **ID:** This column shows the ID of the attribute.
- **Name:** This column contains a mix of synonyms for origins and IDs.
- **Value:** This column contains the value of the attribute, if it exists.
- **Category:** This column has the value "absolute" for all attributes that are only used for data analysis.

## EntitiesAttributeRel

This Excel sheet shows the relationship between entities and their attributes.

- **Origin1:** This column contains the origin of the first entity or entity attribute.
- **ID1:** This column contains the ID of the first entity or entity attribute.
- **Origin2:** This column contains the origin of the second entity or entity attribute.
- **ID2:** This column contains the ID of the second entity or entity attribute.

## ActivityAttributes

This sheet of the dataset shows the activity attributes.

- **Activity_Attributes_ID:** This column contains a foreign key that relates to the event log sheet.
- **Activity:** This column shows the activity, corresponding to the "Activity" column in the event log sheet.
- **Activity_Synonym:** This column shows the synonym for the activity, with a corresponding column of the same name in the event log sheet.
- **Attribute:** This column contains the attributes.
- **Attribute_Value:** This column contains the values of the attributes.

## ActivitiesDomain

This sheet contains the domain of activities, which consists of only one column.

## ICD

This sheet of our dataset contains an excerpt of our ICD codes.

- **ICD_Origin:** This column contains values for all ICD entries. It is an auxiliary column used solely for data analysis.
- **ICD_Code:** This column shows the ICD codes.
- **ICD_Version:** This column shows the version of the ICD codes.
- **ICD_Code_Title:** This column shows the titles of the ICD codes.

## SCT_Node

This sheet of our dataset contains an excerpt of our SNOMED CT concept codes.

- **SCT_ID:** This column contains the SNOMED CT ID.
- **SCT_Code:** This column is an auxiliary column used in this sheet, not related to SNOMED CT terminology.
- **SCT_DescriptionA_Type1:** This column shows the description of SNOMED CT IDs with their semantic tag in parentheses.
- **SCT_DescriptionA_Type2:** This column shows the description of SNOMED CT IDs without their semantic tag in parentheses.
- **SCT_DescriptionB:** This column shows another description of SNOMED CT IDs, which exists only for some of them.
- **SCT_Semantic_Tags:** This column contains the semantic tags of SNOMED CT IDs.
- **SCT_Type:** This column contains the type of SNOMED CT, used to categorize SNOMED CT into three categories: root (only one ID, 138875005), top-level concept (we have 18 SNOMED CTs), and concept (all other IDs besides root and top-level concepts).
- **SCT_Level:** This is an index we used that shows the distance of a SNOMED CT ID from the root SNOMED CT ID (138875005). Sometimes, there are different paths to navigate from a SNOMED CT ID to the root SNOMED CT ID, so it may have more than one level. This index facilitates and enhances the speed of queries.

## SCT_REL

This sheet shows the relationships between SNOMED CT concepts.

- **SCT_ID_1:** The ID of the first SNOMED CT concept node.
- **SCT_Code_1:** The code of the first SNOMED CT concept node.
- **SCT_ID_2:** The ID of the second SNOMED CT concept node.
- **SCT_Code_2:** The code of the second SNOMED CT concept node.

## DK3

This sheet shows the constrained node mappings derived from the MIMIC-IV dataset, which relate each Disorder_ID (an attribute of multimorbidity) to each ICD code.

- **Disorder_ID:** This column shows the disorder attribute identifier.
- **ICD_Code:** This column contains the ICD code.

## DK4

This sheet shows the constrained node mappings derived from "OHDSI Athena" for relating ICD codes to SNOMED CT.

- **ICD_Code:** This column contains the ICD codes.
- **SCT_ID:** This column contains the SNOMED CT IDs.

## DK5

This sheet shows the constrained node mappings derived manually by searching to relate activities to SNOMED CT concepts.

- **Activity:** This column shows the activity, corresponding to the "Activity" column in the event log sheet.
- **Activity_Synonym:** This column shows the synonym for the activity, with a corresponding column of the same name in the event log sheet.
- **SCT_ID:** This column contains the SNOMED CT IDs.
- **SCT_Code:** This column contains the SNOMED CT codes.

## DK6_1

This sheet shows the constrained node mappings derived manually by searching to relate activities to domains.

- **Activity:** This column shows the activity, corresponding to the "Activity" column in the event log sheet.
- **Activity_Synonym:** This column shows the synonym for the activity, with a corresponding column of the same name in the event log sheet.
- **Activity_Domain:** This column shows the domain of activities.

## DK6_2

This sheet shows the constrained node mappings derived manually by searching to relate the domain of activities to SNOMED CT concepts.

- **Activity_Domain:** This column shows the domain of activities.
- **SCT_ID:** This column contains the SNOMED CT IDs.
- **SCT_Code:** This column contains the SNOMED CT codes.

## DK7

This sheet shows the constrained node mappings derived from training a supervised machine learning model to relate activity instance identifiers to disorder identifiers. By using this sheet, we can include another entity (disorder) in addition to the Patient and Admission entities in our analysis.

- **Activity_Instance_ID:** This column contains the activity instance identifiers. This foreign key can be related to the event log sheet.
- **Disorders_ID:** This column contains the identifiers of disorder attributes.


## Project


## Project


## Project

## Project

## Project

## Project


## Project


## Project

## Project

## Project

## Project


## Project


## Project

## Project

## Project

## Project


## Project


## Project


## Project

## Project

## Project


## Project


## Project

# Project Title


# Project Title


# Project Title


# Project Title



<img src="./README_resources/CEKG_02.jpeg" alt="Alt text" width="600" height="600"/>
