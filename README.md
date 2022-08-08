# Detecting Deviations in Oceanic Airspace

## Motivation

There are standards and regulations set related to the amount and type of separation that should exist between aircraft flying through Oceanic Airspace. It is important to review data collected about aircraft as it flies through Oceanic Airspace to ensure that regulations and standards are being met. In the rare instances that there was a potential loss of separation which went aginst the set standards, it is important to have those events flagged for further study. That is where this project comes in to play. The aim of this project is to check records of flights in the Pacific Oceanic Airspace for any potential losses of separation that may have occured between aircraft so that they can be further analyzed. 

## Functionality Summary
The project code can be broken into four main components: Data Input, Data Clean Up, Report Generation, Report Storage.

In the folders there is more detail regarding the specific functions that make these tasks happen, but at a high level, here is the functionality of the four main components:
1. Data Input
    - Pull in raw data file from an S3 bucket and store for usage throughout the script
2. Data Cleanup 
    - Filter out from the raw data only the attributes of interest
    - Per loss of separation standards in the Pacific Airspace: remove flights below the 240 flight level, remove flights in the Hawaii Airspace
    - Additional cleanup specific to this project: Date time formatting, listing the direction of the aircraft, filtering out only records at the start of each minute
3. Report Generation
    - Proximity Report: Find aircraft that come within 25 nautical miles of each other and compute their flight level distance. This is checked for every minute from 00:00 to 23:59 then flags the ones that have a height difference less than 1000ft and less than 400ft. 
    - Loss of Separation Reports: Provides information of what occured 5 minutes before and after the potential separation for further analysis and for visualization of the flight paths during those times. 
4. Report Storage
    - Reports are sent back to an S3 Bucket for storage as CSV files. 

## Output
The output is 5 reports that can be used for further analysis:
1. proximityReport - All of the flight pairs that were within 25 nautical miles in a 1 minute increment as well as the aircraft information and their flight level difference.
2. separation400Report - Flight details for potential loss of separations with a flight level difference less than 400 ft. 
3. separation1000Report -Flight details for potential loss of separations with a flight level difference less than 1000 ft. 
4. visualization400Report - Flight details for potential loss of separations with a flight level difference less than 400 ft in a format for Tableau to visualize. 
5. Visualization1000Report - Flight details for potential loss of separations with a flight level difference less than 400 ft in a format for Tableau to visualize.

## Code Example
![image](https://user-images.githubusercontent.com/72180165/183317270-1f4e5462-5d8d-48ae-a8c8-7afd63071f18.png)

## Output Example
CSV files stored in an S3 bucket:
![image](https://user-images.githubusercontent.com/72180165/183317318-692f2e78-7704-400a-87fd-a462f25af5cc.png)

These reports can then be used to create other visualizations with Python libraries such as Folium:
![image](https://user-images.githubusercontent.com/72180165/183317411-aba92680-61eb-45d4-8a70-8844bc0a062e.png)

Or they can be uploaded to Tableau to create animated visualizations and dashboards:
![image](https://user-images.githubusercontent.com/72180165/183317458-89f38358-0244-4286-911b-75f8a09dd9f2.png)


## Code specifications
The code is written in Python. The following libraries are used in the code: pandas, geopandas, geopy, shapely, dask, pandasql, numpy, math, datetime, sagemaker, boto, os

## Running the script
The script is optimized to work on Amazon Web Service's (AWS) Sagemaker. Clone the repository, get the script into Sagemaker, and run the code. 

NOTE: 
  - A prerequiste would be having the raw data already setup in an S3 bucket
  - Users need to specify the bucket where the raw data will be pulled from

At its core it is a Jupyternotebook that can be outside of Sagemaker in a standalone Jypyternotebook or as a standlone Python script but modifications would need to be done to get the data input and to store the data output. 

## Build Status
The project code is currently conplete. No further updates or maintenance will be completed on this code. 

## Authors 
Developers: Lorenzo Flores, Chen Yuan Lee, Erick Orellana, Pakawan Thongtang, Han Zhang

Advisors: Federal Aviation Administration (FAA)  Air Traffic Organization (ATO), Dr. Rajesh Aggarwal
