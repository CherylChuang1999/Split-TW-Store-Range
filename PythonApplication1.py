from numpy import isnan
import pandas as pd
import os
import pyodbc
from datetime import date
from configparser import ConfigParser

config_object = ConfigParser()

config_object.read("config.ini")

SERVER = config_object["POSHQ"]["SERVER"]
DATABASE = config_object["POSHQ"]["DATABASE"]
USERNAME = config_object["POSHQ"]["USERNAME"]
PASSWORD = config_object["POSHQ"]["PASSWORD"]

connectionString = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
conn = pyodbc.connect(connectionString)
query = """
with TRT
AS (
	Select BeginNo = Case
		When BeginNo is NULL and RangeType = 0 Then 0
		Else BeginNo
		End
	,EndNo = Case
		When EndNo is NULL and RangeType = 0 Then 9999999999
		Else EndNo
		End
	,StoreID 
	,StreetID
	,RangeType = Case
		When RangeType = 0 Then '2'
		When RangeType = 1 Then '3'
		Else '1'
	End
	From [POSHQ].[dbo].[StoreScope]
)

SELECT TRT.StoreID
      ,s.Name as StreetName
      ,c.Name as CityName
      ,st.Name as RegionName
	  ,TRT.BeginNo
	  ,TRT.EndNo
	  ,CONCAT(st.Name, c.Name) as RegionCity
	  ,TRT.RangeType
  FROM TRT
  JOIN [POSHQ].[dbo].[Street] s on TRT.StreetID = s.StreetID
  JOIN [POSHQ].[dbo].[City] c on s.CityID = c.CityID
  JOIN [POSHQ].[dbo].[State] st on c.StateID = st.StateID
  JOIN [POSHQ].[dbo].[Store] sto on sto.StoreID = TRT.StoreID and sto.Status = 1
  order by TRT.StoreID;
""";

dfCity = pd.read_excel('City.xlsx')
dfPostal = pd.read_excel('Postal.xlsx')
dfRecords = pd.read_sql(query, conn)
dfRecordsMergeCity = pd.merge(dfRecords, dfCity, how="left", on=["RegionCity", "RegionCity"], validate="m:1");
dfRecordsCityMergePostal = pd.merge(dfRecordsMergeCity, dfPostal, how="left", on=["RegionCity", "RegionCity"], validate="m:1");
storeStreetDirectory = './Data';

if not os.path.exists(storeStreetDirectory):
  os.mkdir(storeStreetDirectory)

defaultStreetRow = {
    'Street_Code': 1000000000,
    'Pre_Direction': '',
    'Street_Name': '',
    'Post_Direction': '',
    'City_Code': 200000,
    'Added_Manually': 0,
    'Added_By': 'admin',
    'Added': '2023-08-11 15:18:56.56',
    'Local_Name': ''
}
defaultSubstreetRow = {
     'Sequence_Number': 1000000000,
     'Street_Code': 1000000000,
     'Pri_Lower_Bound': 0,
     'Pri_Upper_Bound': 9999999999,
     'Pri_Odd_Even': 2,
     'Sec_Lower_Bound': 0,
     'Sec_Upper_Bound': 9999999999,
     'Sec_Odd_Even': 2,
     'Car_Route': '',
     'Zip4Lower_Bound': '',
     'Zip4Upper_Bound': '',
     'Location_Code': '',
     'Postal_Code': '',
     'LDA_Code': '',
     'LDA_Added_By': '',
     'LDA_Added': '',
     'Added_Manually': 0,
     'Added_By': 'Admin',
     'Added': '2023-08-14 11:41:05.05',
     'Updated_By': '',
     'Updated': '',
     'Pri_Lower_Bound_Dec': 0,
     'Pri_Upper_Bound_Dec': 9999999999,
     'Sec_Lower_Bound_Dec': 0,
     'Sec_Upper_Bound_Dec': 9999999999,
     'Is_In_Delivery_Area': 1
}

dfDefaultStreet = pd.DataFrame([defaultStreetRow])
dfDefaultSubstreet = pd.DataFrame([defaultSubstreetRow])


print('Please wait for a moment, it will take a few minutes to complete.')

try :
    for r in dfRecordsCityMergePostal['StoreID'].unique():
        storeDirectory = storeStreetDirectory + r"\\" + str(int(r)) + r"\\"
        streetsPath = storeDirectory + 'streets.txt'
        subtreetsPath = storeDirectory + 'substreets.txt'
        if not os.path.exists(storeDirectory):
           os.makedirs(storeDirectory)
        data = dfRecordsCityMergePostal.loc[dfRecordsCityMergePostal['StoreID'] == r]
        streetsData = data.drop_duplicates(subset=['StreetName','City Code'])
        dfDefaultStreet.to_csv(streetsPath, index=False, float_format='%.0f', mode='a', header=False)
        # Build the streets file first, then build the substreets file
        for index, row in streetsData.iterrows():
           streetrow = {
                'street_code': 100000000 + index,
                'pre_direction': '',
                'street_name': row['StreetName'],
                'post_direction': '',
                'city_code': row['City Code'],
                'added_manually': 1,
                'added_by': 'System',
                'added': date.today().strftime('%Y-%m-%d %H:%M:%S'),
                'local_name': ''
           }
           streetRow = pd.DataFrame([streetrow])
           streetRow.to_csv(streetsPath, index=False, float_format='%.0f', mode='a', header=False)

        for index, row in data.iterrows():
           streetCode = 100000000 + streetsData[streetsData['StreetName'] == row['StreetName']].index[0]
           substreetrow = {
                'Sequence_Number': 100000000 + index,
                'Street_Code': streetCode,
                'Pri_Lower_Bound': row['BeginNo'],
                'Pri_Upper_Bound': row['EndNo'],
                'Pri_Odd_Even': row['RangeType'],
                'Sec_Lower_Bound': '',
                'Sec_Upper_Bound': '',
                'Sec_Odd_Even': '',
                'Car_Route': '',
                'Zip4Lower_Bound': '',
                'Zip4Upper_Bound': '',
                'Location_Code': row['Location_Code'],
                'Postal_Code': row['PostalCode'],
                'LDA_Code': '',
                'LDA_Added_By': '',
                'LDA_Added': '',
                'Added_Manually': 1,
                'Added_By': 'System',
                'Added': date.today().strftime('%Y-%m-%d %H:%M:%S'),
                'Updated_By': '',
                'Updated': '',
                'Pri_Lower_Bound_Dec': row['BeginNo'],
                'Pri_Upper_Bound_Dec': row['EndNo'],
                'Sec_Lower_Bound_Dec': '',
                'Sec_Upper_Bound_Dec': '',
                'Is_In_Delivery_Area': 1
           }
           substreetRow = pd.DataFrame([substreetrow])
           substreetRow.to_csv(subtreetsPath, index=False, float_format='%.0f', mode='a', header=False)
        
        dfDefaultSubstreet.to_csv(subtreetsPath, index=False, float_format='%.0f', mode='a', header=False)
except Exception as error:
    print("An exception occurred:", error)
finally:
    print('Complete.')