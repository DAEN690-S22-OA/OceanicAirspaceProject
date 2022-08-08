#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Install libraries
get_ipython().system('pip install geopy')
get_ipython().system('pip install shapely ')
get_ipython().system('pip install pandasql')


# In[ ]:


#Import Libraries
import pandas as pd
import geopy as gy
import shapely as sy
import dask.dataframe as dd
import pandasql as pq
from shapely.geometry import Point, Polygon
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from datetime import timedelta
import sagemaker, boto3, os


# In[ ]:


#Setup the path for the file containing ther raw data
bucket = 'daen-690-pacific-deviations/raw-data' #Bucket name
data_key = 'TOMRDate=2021-12-28.csv' #Name of the CSV file 
data_location = 's3://{}/{}'.format(bucket, data_key)

#Import all of the raw data 
rawData_df = dd.read_csv(data_location, assume_missing=True)


# In[ ]:


#Function to filter out the needed attribues, rename, and change flight level scale
def filterAttributes():
    #New dataframe with selected attributes from the raw data
    airspaceData_df = rawData_df[["FRN73TMRPDateTimeOfMessageRec","FRN131HRPWCFloatingPointLat","FRN131HRPWCFloatingPointLong",
                     "FRN145FLFlightLevel", "FRN170TITargetId","RESHSelectedHeading","FRN80TATargetAddress",
                     "FRN161TNTrackNumber"]]

    #Rename columns to make it easier to read
    airspaceData_df = airspaceData_df.rename(columns={'FRN73TMRPDateTimeOfMessageRec': 'DateTime', 
                                                      'FRN131HRPWCFloatingPointLat': "Latitude", 
                                                      'FRN131HRPWCFloatingPointLong': "Longitude", 
                                                      'FRN145FLFlightLevel': "FlightLevel", 
                                                      'FRN170TITargetId': "TargetID", 
                                                      'RESHSelectedHeading': "SelectedHeading", 
                                                      'FRN80TATargetAddress': "TargetAddress",
                                                      'FRN161TNTrackNumber': "TrackNumber"})
    
    
    #Change flight level scale to feet (FL1 = 100 ft)
    airspaceData_df['FlightLevel'] = airspaceData_df['FlightLevel'].apply(lambda x: x * 100, meta=('FlightLevel', 'float64'))
    
    #Switch from dask dataframe to pandas
    airspaceData = airspaceData_df.compute()
    
    return airspaceData


# In[ ]:


#Function to format date and time  
def timeFormatting(allAircraftData):
    
    #Reformatting string to conver to time stamp
    char = ['T','Z']
    for x in char:
        allAircraftData["DateTime"] = allAircraftData["DateTime"].str.replace( x ," ")

    # Saving the Formatted Datetime
    allAircraftData["DateTime"] = pd.to_datetime(allAircraftData["DateTime"], format="%Y-%m-%d %H:%M:%S")
    
    # Create 4 new columns for Hour, Minute, Second and Microsecond
    allAircraftData["Hour"] = allAircraftData["DateTime"].dt.hour
    allAircraftData["Minute"] = allAircraftData["DateTime"].dt.minute
    allAircraftData["Second"] = allAircraftData["DateTime"].dt.second
    allAircraftData["Day"] = allAircraftData["DateTime"].dt.strftime('%Y-%m-%d')
    
    # Reorder columns
    allAircraftData = allAircraftData[["DateTime","Day","Hour","Minute","Second","Latitude","Longitude","FlightLevel",
                                   "TargetID","SelectedHeading","TargetAddress",
                                   "TrackNumber"]]
    return allAircraftData


# In[ ]:


#Function to filter for only those flights at or above flight level 240
def dataFiltering(): 
    
    global airspaceData
    global allAircraftData

    #Remove anything below FL240
    airspaceData = allAircraftData[(allAircraftData['FlightLevel'] >= 24000)]

    #Keep only records for the first 5 seconds to speed up processing time 
    airspaceData = airspaceData[(airspaceData['Second'] < 5)]


# In[ ]:


#Function to filter out anything in the Hawaii airspace
def removeHISpace():
    
    global airspaceData
    
    #Coordinates for Hawaii airspace
    v0 = (26.14472222, -158.62194444) 
    v1 = (26.105, -160.63166667)
    v2 = (25.67611111, -161.69111111)
    v3 = (25.05666667, -162.64972222)
    v4 = (24.16889, -163.26638889)
    v5 = (23.25833, -163.855)
    v6 = (22.20555556, -163.91444444)
    v7 = (21.1511111, -163.9144444)   
    v8 = (20.11666667, -163.3)
    v9 = (19.65805556,-162.69944444)
    v10 = (19.415, -162.38361111)
    v11 = (18.40777778, -160.81416667)
    v12 = (18.0525, -160.26972222)
    v13 = (17.75583333, -159.53888889)
    v14 = (17.17055556, -157.75666667) 
    v15 = (17.805,-156.06805556)
    v16 = (18.10888889, -155.71166667)
    v17 = (19.14222222, -154.48333333)
    v18 = (19.22293333, -151.87963333)
    v19 = (20.69694444, -151.01916667) 
    v20 = (21.54777778, -151.46638889)
    v21 = (22.34416667,-151.88527778)
    v22 = (23.02416667, -152.57777778)
    v23 = (23.78055556, -153.36611111)
    v24 = (24.29583333, -154.25)
    v25 = (24.72138889, -155.26305556)
    v26 = (25.19583333, -156.42111111)

    # Polygon
    coords = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26]
    poly = Polygon(coords)
    
    #Sort flights into what is in the airspace and what is not
    hawaiiAir = []

    for loc in range(0,len(airspaceData)):
        p1 = Point(airspaceData.iloc[loc][5], airspaceData.iloc[loc][6])
        hawaiiAir.append(p1.within(poly))

    airspaceData['nearHawaii'] = hawaiiAir
    
    #Filter out only the ones in the airspace
    airspaceData = airspaceData[(airspaceData['nearHawaii'] == False)]
    airspaceData = airspaceData.drop(columns=['nearHawaii'])


# In[ ]:


# Function to remove flights that appear in the record only one time
def removeSingleoccurrence():
    
    global airspaceData
    
    #Find the target IDs that only appear once
    removeFlights = airspaceData['TargetID'].value_counts().rename_axis('targetID').reset_index(name='counts')
    removeFlights = removeFlights[(removeFlights['counts'] == 1)].reset_index(drop = True)
    
    #Remove the flights that only appear once
    location = 0
    for x in removeFlights:
        removeID = removeFlights.loc[location][0]
        airspaceData = airspaceData[airspaceData.TargetID.str.contains(removeID) == False].reset_index(drop=True)
        location = location + 1


# In[ ]:


# Function to set the direction of aircraft
def aircraftDirection():
    
    global airspaceData
    
    # Replace missing value with -1
    airspaceData['SelectedHeading'] = airspaceData['SelectedHeading'].fillna(-1)
    
    # Assign Direction "E" for 0-180 degree, "W" for 180-360 degree, "NA" is record with null values 
    conditionlist = [
        (airspaceData['SelectedHeading'] < 0) ,
        (airspaceData['SelectedHeading'] >= 0) & (airspaceData['SelectedHeading'] <180),
        (airspaceData['SelectedHeading'] > 180)]
    choicelist = ['NA', 'E', 'W']
    airspaceData['Direction'] = np.select(conditionlist, choicelist)


# In[ ]:


# Function to query Hour and Minute from 'airspaceData' table
def minuteFilter(HourCounter,MinuteCounter):
    
    global airspaceData

    #create SQL query for flights between the start and end time
    sql1 = "SELECT *, min(Second) FROM airspaceData WHERE Hour = '{0}' and Minute = '{1}' GROUP BY TargetID ORDER BY TargetID, Second".format(HourCounter, MinuteCounter)

    #Run query and store results
    recordsInMinute = pq.sqldf(sql1, globals())
    del recordsInMinute['min(Second)']

    return (recordsInMinute.sort_values('Longitude').reset_index(drop=True))


# In[ ]:


# Function for caluculating distance using 'Haversine formula'
def distance_d(point0,pointX):
    
    # The function "radians" is found in the math module
    LoA = radians(point0[1])  
    LoB = radians(pointX[1])
    LaA=  radians(point0[0])  
    LaB = radians(pointX[0]) 
    # The "Haversine formula" is used.
    D_Lo = LoB - LoA 
    D_La = LaB - LaA 
    P = sin(D_La / 2)**2 + cos(LaA) * cos(LaB) * sin(D_Lo / 2)**2  
    Q = 2 * asin(sqrt(P))   
    
    # The earth's radius in kilometers.
    R_km = 6371  
 
    # Change the kilometer to  nautical miles
    R_nm = R_km*0.539956803

    # Then we'll compute the outcome.
    return(Q * R_nm)


# In[ ]:


# Function to set up boundary at 25 nm by longitude 
def limit_lon(point0):
    
    LaA = radians(point0[0])
    # calculate distance for one degree of longitude at this latitude(LaA)
    onedeg_long = cos(LaA)*(69.172*0.868976242)
    add = 25/onedeg_long 
    pointlimit = (point0[0],point0[1]+add)
    
    # return Longitude of pointlimit
    return pointlimit[1]


# In[ ]:


# Function to select, merge and add the values from analyzing Longitude and Latitude
def newDF(OrderDF,x,y,d):
    """DF is Long/LatitudeOrderDF
       x = long/latpoint_a
       y = long/latpoint_b
       d = long/latdistance_ab"""
    # select rows that index is in list 'point_a', 'point_b'
    A = OrderDF.loc[x,['DateTime','Day','Hour','Minute','Second','Latitude','Longitude','FlightLevel',
                             'TargetID', 'SelectedHeading', 'TargetAddress','Direction']]
    B = OrderDF.loc[y,['DateTime','Day','Hour','Minute','Second','Latitude','Longitude','FlightLevel',
                             'TargetID', 'SelectedHeading', 'TargetAddress','Direction']]
    # Join 2 tables by the "TargetID" of point a (for the uniquness)
    OrderResult = pd.merge(A.reset_index(drop=True),B.reset_index(drop=True),left_index=True, right_index=True)
    # add distance column
    OrderResult['Distance'] = d
    return OrderResult


# In[ ]:


# Calculate the distance of the points closest to each other by longitidue and latitude
def proximityCalc(LongitudeOrderDF):
    
    #Store the points of interest
    longpoint_a = []
    longpoint_b = []
    longdistance_ab = []

    #loop through the list to find the proximity of aircraft to each other
    for a in LongitudeOrderDF.index:
        for n in range(1,len(LongitudeOrderDF)):
            b = a+n
            if b < len(LongitudeOrderDF):
                point0 = LongitudeOrderDF.loc[a,'Latitude'], LongitudeOrderDF.loc[a,'Longitude']
                pointX = LongitudeOrderDF.loc[b,'Latitude'], LongitudeOrderDF.loc[b,'Longitude']
                # Check if longitude of pointX is within the boundary
                if pointX[1] <= limit_lon(point0): 
                    distance = distance_d(point0,pointX)
                    # Check distance within 25 nm
                    if distance <= 25: 
                        longpoint_a.append(a)
                        longpoint_b.append(b)
                        longdistance_ab.append(distance)
                    else:
                        break
        
    # Apply function to select and merge data frame
    Resultsdf = newDF(LongitudeOrderDF,longpoint_a, longpoint_b,longdistance_ab)

    return (Resultsdf)


# In[ ]:


# Function for calculating height differences
def distanceCalc(resultsDF):
    
    #Store results 
    heightDifference = []
    potentialLoss1000 = []
    potentialLoss400 = []
    
    # Loop through every rows of the input dataframe
    for counter in range(0,len(resultsDF)):
        difference = abs((resultsDF['FlightLevel_x'][counter]) - (resultsDF['FlightLevel_y'][counter]))
        heightDifference.append(difference)
        # checkif the difference < 1000
        if difference < 1000:
            potentialLoss1000.append('True')
            # check if the difference <= 400
            if difference <= 400:
                potentialLoss400.append('True')
            else:
                potentialLoss400.append('False')
        else:
            potentialLoss1000.append('False')
            potentialLoss400.append('False')
            
    # Add columns showing hightdifference(ft), potentialLoss400 and 1000
    resultsDF['HeightDifference_ft'] = heightDifference
    resultsDF['potentialLoss400'] = potentialLoss400
    resultsDF['potentialLoss1000'] = potentialLoss1000

    return (resultsDF)


# In[ ]:


# Function for removing duplicate pairs of aircrafts regardless of order
def removeProximityDups(proximityReport):
    
    #Remove any records that do not have a target ID 
    proximityReport['TargetID_x'].replace('', np.nan, inplace=True)
    proximityReport['TargetID_y'].replace('', np.nan, inplace=True)

    proximityReport.dropna(subset=['TargetID_x'], inplace=True)
    proximityReport.dropna(subset=['TargetID_y'], inplace=True)
    
    # sorted targetID in new column 'list_target', then delete the duplicate ones
    proximityReport['list_target'] = proximityReport.apply(lambda row: tuple(sorted([row['TargetID_x']]+[row['TargetID_y']])), axis = 1)
    proximityReport = proximityReport.drop_duplicates(subset = 'list_target',keep = 'first').reset_index(drop = True)
    proximityReport.drop('list_target', axis=1, inplace=True)
    
    proximityReport = proximityReport[proximityReport.TargetID_x.str.contains(' ') == False].reset_index(drop=True)
    proximityReport = proximityReport[proximityReport.TargetID_y.str.contains(' ') == False].reset_index(drop=True)

    return proximityReport


# In[ ]:


# Function to loop through every Hour and Minute, and call function to claculate proximity and hight difference
def getProximityReport():
    proximityReport = pd.DataFrame()

    for HourCounter in range(0,24):
        #Create table for the minute
        for MinuteCounter in range(0,60):
            #Create table for the minute
            recordsByMinuteDF = minuteFilter(HourCounter,MinuteCounter)

            #calculate proximity
            resultsDF = proximityCalc(recordsByMinuteDF)

            if resultsDF.empty == True:
                # if the results dataframe is empty, then break out of for-loop
                break
            else:
                #Calculate distance
                resultsDF = distanceCalc(resultsDF)
                #Add the results for this minute to the overall results 
                proximityReport = pd.concat([proximityReport, resultsDF], ignore_index=True)

    #Remove any duplicate entries
    proximityReport = removeProximityDups(proximityReport)
    
    return proximityReport


# In[ ]:


# Function to get only results where potentialLoss at 400ft is True
def get400candidate(proximityReport):
    
    #Find records from the proximity report that have been marked as potential loss of separation at 400 ft
    LossCandidates400 = proximityReport.loc[(proximityReport['potentialLoss400'] == 'True')]
    LossCandidates400 = LossCandidates400.reset_index()
    LossCandidates400 = LossCandidates400.drop(columns=['index'])
    
    if len(LossCandidates400) > 0:
        #remove duplicate pairs
        LossCandidates400['list_target'] = LossCandidates400.apply(lambda row: tuple(sorted([row['TargetID_x']]+[row['TargetID_y']])), axis = 1)
        LossCandidates400 = LossCandidates400.drop_duplicates(subset = ['list_target'],keep = 'last').reset_index(drop = True)
        LossCandidates400.drop('list_target', axis=1, inplace=True)

    return LossCandidates400


# In[ ]:


# Function to get only results where potentialLoss at 1000ft is True
def get1000candidate(proximityReport):
    
    #Find records from the proximity report that have been marked as potential loss of separation at 1000 ft
    LossCandidates1000 = proximityReport.loc[(proximityReport['potentialLoss1000'] == 'True')]
    LossCandidates1000 = LossCandidates1000.reset_index()
    LossCandidates1000 = LossCandidates1000.drop(columns=['index'])
    
    if len(LossCandidates1000) > 0:
        #remove duplicate pairs
        LossCandidates1000['list_target'] = LossCandidates1000.apply(lambda row: tuple(sorted([row['TargetID_x']]+[row['TargetID_y']])), axis = 1)
        LossCandidates1000 = LossCandidates1000.drop_duplicates(subset = ['list_target'],keep = 'last').reset_index(drop = True)
        LossCandidates1000.drop('list_target', axis=1, inplace=True)

    return LossCandidates1000


# In[ ]:


# Function to get the data for the flight at +/- 5 minutes from when the loss of separation was flagged
def recordsTable(instancesAtLevel, x):
    
    #Finds the flights of interest
    flight_x = instancesAtLevel['TargetID_x'][x]
    flight_y = instancesAtLevel['TargetID_y'][x]
    
    #Minutes before and after
    occuranceTime = instancesAtLevel['DateTime_x'][x]
    date_format_str = '%Y-%m-%d %H:%M:%S.%f'
    occuranceTime = datetime.strptime(occuranceTime, date_format_str)
    
    n = 5 #Number of minutes - can be increased or decreased as needed
    start_time = occuranceTime - timedelta(minutes=n)
    end_time = occuranceTime + timedelta(minutes=n)

    #Getting the records from the raw data for +/- N minutes from the flagged separation
    flightInformation = allAircraftData.loc[((allAircraftData['TargetID'] == flight_x) | (allAircraftData['TargetID'] == flight_y)) & 
                                          ((allAircraftData['DateTime'] >= start_time) & (allAircraftData['DateTime'] <= (end_time)))]
  
    #Assigning an ID to the separations for sorting/filtering as needed
    flightInformation = flightInformation.assign(SeparationEntry=x)

    return flightInformation.sort_values(by=['SeparationEntry','DateTime', 'TargetID'])  


# In[ ]:


# Function to fill missing second with linear interpolation 
def fillSecond(data_x,data_y):
    '''This function transform data of target y to be 
      on the same minute and second as target x'''
    # filled with NA in data_y if second_x are not in second_y 
    Y = data_y.groupby('Minute')['Second'].apply(list).reset_index(name='list')
    for i in data_x.index:
        min_x = data_x.loc[i,'Minute']
        sec_x = data_x.loc[i,'Second']
        for n in range(0,len(Y)):
            min_y = Y.loc[n,'Minute']
            if min_x == min_y:
                listsec = Y.loc[n,'list']
                if (sec_x not in listsec):
                    ydict = {'Minute': min_x, 'Second': sec_x, 
                   'TargetID': data_y.loc[0,'TargetID']}
                    data_y = data_y.append(ydict, ignore_index = True)

    # fill NA with linear interpolation method
    y_interp = data_y.sort_values(by=['Minute','Second']).interpolate(method='linear', limit_direction ='forward')
    y_transformed = y_interp.interpolate(method='linear', limit_direction ='backward')
    return y_transformed


# In[ ]:


# Function to put data_x and data_y on the same time scale
def transformTable(flightData):
    for i, id in enumerate(flightData['TargetID'].unique()):
        if i == 0:
            data_x = flightData[(flightData['TargetID']== id)].reset_index(drop = True)
        else:
            data_y = flightData[(flightData['TargetID']== id)].reset_index(drop = True)

    data_x = data_x[['SeparationEntry','DateTime','Day','Hour','Minute','Second','Latitude','Longitude','FlightLevel','TargetID','SelectedHeading']]
    data_y = data_y[['Minute','Second','Latitude','Longitude','FlightLevel','TargetID','SelectedHeading']]
    
    y_transformed = fillSecond(data_x,data_y)
    
    analyzedTable = pd.merge(data_x,y_transformed,on=['Minute','Second'], how='left')

    #Remove records that the interpolation stored an "NA" for in the latitude 
    analyzedTable['Latitude_y'].replace('', np.nan, inplace=True)
    analyzedTable['Latitude_y'].replace('', np.nan, inplace=True)

    analyzedTable.dropna(subset=['Latitude_y'], inplace=True)
    analyzedTable.dropna(subset=['Latitude_y'], inplace=True)
    
    return analyzedTable.reset_index(drop=True)


# In[ ]:


# Function to calculate distance using 'Haversine formula'
def haversineAnalysis(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):

    if to_radians:
        lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])

    a = np.sin((lat2-lat1)/2.0)**2 +         np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    return earth_radius * 2 * np.arcsin(np.sqrt(a))  * 0.539956803 


# In[ ]:


# Function to calculate and append 'LateralDistance' column
def getLateralDist(analyzedTable):

    analyzedTable['LateralDistance'] =     haversineAnalysis(analyzedTable.Latitude_x, analyzedTable.Longitude_x,
                 analyzedTable.Latitude_y, analyzedTable.Longitude_y)
  
    return analyzedTable


# In[ ]:


# Function to calculate and append the Flight Level differnece column 
def flightlevelCalc(analyzedTable):

    flightlevelDifference = []

    #Subtracting the flight levels to get the difference
    for counter in range(0,len(analyzedTable)):
        Diff = abs((analyzedTable['FlightLevel_x'][counter]) - (analyzedTable['FlightLevel_y'][counter]))
        flightlevelDifference.append(Diff)
        
    analyzedTable['FlightLevelDifference'] = flightlevelDifference

    return analyzedTable


# In[ ]:


# Function to identify direction of aircraft
def getDirection(analyzedTable):

    # Direction
    conditionsX = [(analyzedTable.iloc[-2]['Longitude_x'] - analyzedTable.iloc[0]['Longitude_x'] < 0),(analyzedTable.iloc[-2]['Longitude_x'] - analyzedTable.iloc[0]['Longitude_x'] > 0)]

    # create a list of the values we want to assign for each condition
    values = ['W', 'E']

    # create a new column and use np.select to assign values to it using our lists as arguments
    analyzedTable['Direction_x'] = np.select(conditionsX, values)

    conditionsY = [
      (analyzedTable.iloc[-2]['Longitude_y'] - analyzedTable.iloc[0]['Longitude_y'] < 0),
      (analyzedTable.iloc[-2]['Longitude_y'] - analyzedTable.iloc[0]['Longitude_y'] > 0)
      ]

    # create a list of the values we want to assign for each condition
    values = ['W', 'E']

    # create a new column and use np.select to assign values to it using our lists as arguments
    analyzedTable['Direction_y'] = np.select(conditionsY, values)

    #Updated sorting for reasier reading
    analyzedTable = analyzedTable[analyzedTable.columns[[0,1,2,3,4,5,6,7,8,9,10,18,11,12,13,14,15,19,16,17]]]

    return analyzedTable


# In[ ]:


# Function to get separation report contains information at loss of separation and 5 minutes before and after
def getSeparationReports(instancesAtLevel):
    
    separationReport = pd.DataFrame()

    for x in range(0,len(instancesAtLevel.index)):
        #Get the data for the flight at +/- 5 minutes 
        flightData = recordsTable(instancesAtLevel, x)

        #Format the table for output
        analyzedTable = transformTable(flightData)

        #Compute/assign lateral separation, height separation, and direction
        analyzedTable = getLateralDist(analyzedTable)
        analyzedTable = flightlevelCalc(analyzedTable)
        analyzedTable = getDirection(analyzedTable)

        #Add table to the results 
        separationReport = pd.concat([separationReport, analyzedTable], ignore_index=True)

    return separationReport


# In[ ]:


# Function to get information of only TargetID x
def flightXInfo(separationData):

    flightX = []
    
    #Getting the information for just the first flight
    for x in range(0, len(separationData.index)):
        values_x = [separationData['SeparationEntry'].loc[x], 
                separationData['DateTime'].loc[x], 
                separationData['FlightLevel_x'].loc[x], 
                separationData['TargetID_x'].loc[x], 
                separationData['Direction_x'].loc[x], 
                separationData['LateralDistance'].loc[x], 
                separationData['FlightLevelDifference'].loc[x], 
                separationData['Longitude_x'].loc[x], 
                separationData['Latitude_x'].loc[x]]
        flightX.append(values_x)

    return flightX


# In[ ]:


# Function to get information of only TargetID y
def flightYInfo(separationData):

    flightY = []

    #Getting the information for just the second flight
    for x in range(0, len(separationData.index)):
        values_y = [separationData['SeparationEntry'].loc[x],
                separationData['DateTime'].loc[x],
                separationData['FlightLevel_y'].loc[x], 
                separationData['TargetID_y'].loc[x], 
                separationData['Direction_y'].loc[x], 
                separationData['LateralDistance'].loc[x], 
                separationData['FlightLevelDifference'].loc[x],
                separationData['Longitude_y'].loc[x],
                separationData['Latitude_y'].loc[x]]
        flightY.append(values_y)

    return flightY


# In[ ]:


# Function to create table for visualization
def getVisTable(Resulttable):
    
    xvalues = pd.DataFrame(flightXInfo(Resulttable))
    yvalues = pd.DataFrame(flightYInfo(Resulttable))
    
    # concatenate by index
    tableToVisualize = pd.concat([xvalues, yvalues], ignore_index=True)
    
    #Renaming columns
    tableToVisualize = tableToVisualize.rename(columns={0: 'SeparationEntry',
                                                      1: 'DateTime', 
                                                      2: "FlightLevel", 
                                                      3: "TargetID", 
                                                      4: "Direction", 
                                                      5: "LateralDistance",
                                                      6: "FLDifference",
                                                      7: "Longitude", 
                                                      8: "Latitude"})

    return tableToVisualize


# In[ ]:


#Get the report at the 1000 level
def getSeparation1000Report(proximityReport):
    
    #Get the potential separations at 1000
    flSeparation1000Report = get1000candidate(proximityReport)    
    
    #Get the flight details if there were any potentail separations at the 1000 level
    if len(flSeparation1000Report) > 0:
        flSeparation1000Report = getSeparationReports(flSeparation1000Report)
        
    #Remove missing data due to there not being recordings for the time frame around the potential separation
    flSeparation1000Report['TargetID_x'].replace('', np.nan, inplace=True)
    flSeparation1000Report['TargetID_y'].replace('', np.nan, inplace=True)

    flSeparation1000Report.dropna(subset=['TargetID_x'], inplace=True)
    flSeparation1000Report.dropna(subset=['TargetID_y'], inplace=True)
    
    return flSeparation1000Report.reset_index(drop=True)


# In[ ]:


#Get the visualization data at the 1000 level
def visualization1000(report1000): 
    
    #If there are any potential separations, get the report with one flight per record/row
    if len(report1000) > 0:
        viz1000Data = getVisTable(report1000)
        #Add a flag to where the potential loss of separation to make the visualizations easier to generate in Tableau
        conditions = [
            (viz1000Data['FLDifference'] < 1000) & (viz1000Data['LateralDistance'] <= 25),
            (viz1000Data['FLDifference'] < 1000) & (viz1000Data['LateralDistance'] > 25),
            (viz1000Data['FLDifference'] >= 1000) & (viz1000Data['LateralDistance'] <= 25),
            (viz1000Data['FLDifference'] >= 1000) & (viz1000Data['LateralDistance'] > 25),
        ]
        values = ['True','False','False','False']
        viz1000Data['potentialLoss'] = np.select(conditions, values)
    else:
        #Create an empty table if no recrds exist 
        column_names = ['SeparationEntry', 'DateTime','FlightLevel','TargetID','Direction','LateralDistance',
                       'FLDifference', 'Longitude', 'Latitude']
        viz1000Data = pd.DataFrame(columns = column_names)
    
    return viz1000Data


# In[ ]:


#Get the report at the 400 level
def getSeparation400Report(proximityReport):
    
    #Get the potential separations at 400
    flSeparation400Report = get400candidate(proximityReport)

    #Get the flight details if there were any potentail separations at the 400 level
    if len(flSeparation400Report) > 0:
        flSeparation400Report = getSeparationReports(flSeparation400Report)
        
        
    #Remove missing data due to there not being recordings for the time frame around the potential separation
    flSeparation400Report['TargetID_x'].replace('', np.nan, inplace=True)
    flSeparation400Report['TargetID_y'].replace('', np.nan, inplace=True)

    flSeparation400Report.dropna(subset=['TargetID_x'], inplace=True)
    flSeparation400Report.dropna(subset=['TargetID_y'], inplace=True)
    
    return flSeparation400Report.reset_index(drop=True)


# In[ ]:


#Get the visualization data at the 400 level
def visualization400(report400):
    
    #If there are any potential separations, get the report with one flight per record/row
    if len(report400) > 0:
        viz400Data = getVisTable(report400)
        #Add a flag to where the potential loss of separation to make the visualizations easier to generate in Tableau
        conditions = [
            (viz400Data['FLDifference'] < 400) & (viz400Data['LateralDistance'] <= 25),
            (viz400Data['FLDifference'] < 400) & (viz400Data['LateralDistance'] > 25),
            (viz400Data['FLDifference'] >= 400) & (viz400Data['LateralDistance'] <= 25),
            (viz400Data['FLDifference'] >= 400) & (viz400Data['LateralDistance'] > 25),
        ]
        values = ['True','False', 'False','False']
        viz400Data['potentialLoss'] = np.select(conditions, values)
    else:
        #Create an empty table if no recrds exist
        column_names = ['SeparationEntry', 'DateTime','FlightLevel','TargetID','Direction','LateralDistance',
                       'FLDifference', 'Longitude', 'Latitude']
        viz400Data = pd.DataFrame(columns = column_names)
    
    return viz400Data


# In[ ]:


# Function to export all report files into S3 bucket
def exportFiles(proximityReport, separation400Report, visualization400Report, separation1000Report, visualization1000Report):
    
    #Get the date for labeling
    reportDate = proximityReport['Day_x'][0]
    
    #Create CSV file for each of the reports
    proximityReport.to_csv('proximityReport.csv', index=False, header=True)
    separation400Report.to_csv('separation400Report.csv', index=False, header=True)
    visualization400Report.to_csv('visualization400Report.csv', index=False, header=True)
    separation1000Report.to_csv('separation1000Report.csv', index=False, header=True)
    visualization1000Report.to_csv('visualization1000Report.csv', index=False, header=True)
    
    #Create path for where the files should be stored
    bucket = sagemaker.Session().default_bucket()
    prefix = "potential-loss-separation-{0}".format(reportDate) 

    #Send the CSV files to the S3 bucket in the path provided above
    boto3.Session().resource('s3').Bucket(bucket).Object(
        os.path.join(prefix, 'data/proximityReport.csv')).upload_file('proximityReport.csv')
    boto3.Session().resource('s3').Bucket(bucket).Object(
        os.path.join(prefix, 'data/separation400Report.csv')).upload_file('separation400Report.csv')
    boto3.Session().resource('s3').Bucket(bucket).Object(
        os.path.join(prefix, 'data/visualization400Report.csv')).upload_file('visualization400Report.csv')
    boto3.Session().resource('s3').Bucket(bucket).Object(
        os.path.join(prefix, 'data/separation1000Report.csv')).upload_file('separation1000Report.csv')
    boto3.Session().resource('s3').Bucket(bucket).Object(
        os.path.join(prefix, 'data/visualization1000Report.csv')).upload_file('visualization1000Report.csv')


# In[ ]:


# Function to call data cleaning steps
def cleanAllAircraftData():
    
    global allAircraftData
    
    allAircraftData = filterAttributes()
    allAircraftData = timeFormatting(allAircraftData)
    


# In[ ]:


# Function to call data cleaning steps for analysis
def cleanAirspaceData():
    
    airspaceData = dataFiltering()
    airspaceData = removeHISpace()
    airspaceData = removeSingleoccurrence()
    airspaceData = aircraftDirection()


# In[ ]:


airspaceData = pd.DataFrame() #Store filtered data
allAircraftData = pd.DataFrame() #Store raw data with only the attributes of interest for the project

def main():
    
    global allAircraftData
    global airspaceData   

    #Functions to call for the data cleanup
    cleanAllAircraftData()
    cleanAirspaceData()

    #Functions to call for the reports
    proximityReport = getProximityReport()
    separation400Report = getSeparation400Report(proximityReport)
    separation1000Report = getSeparation1000Report(proximityReport)
    visualization400Report = visualization400(separation400Report)
    visualization1000Report = visualization1000(separation1000Report)

    #Export and Save reports
    exportFiles(proximityReport, separation400Report, visualization400Report, separation1000Report, visualization1000Report)

if __name__ == "__main__":
    main()

