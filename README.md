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


## Project

# Project Title


# Project Title


# Project Title


# Project Title



<img src="./README_resources/CEKG_02.jpeg" alt="Alt text" width="600" height="600"/>
