from numpy import isnan
import pandas as pd
import os

dfStreets = pd.read_excel('Streets.xlsx');

dfSubstreets = pd.read_excel('Substreets.xlsx');

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

try:
    for s in dfStreets['StoreID'].unique():
        if isnan(s) == 0:
            storeDirectory = storeStreetDirectory + r"\\" + str(int(s)) + r"\\"
            filePath = storeDirectory + 'streets.txt'
            isStoreDirExist = os.path.exists(storeDirectory)
            if not isStoreDirExist:
                os.makedirs(storeDirectory)
            data = dfStreets.loc[dfStreets['StoreID'] == s]
            data = data.drop_duplicates(subset=['Street_Name','City_Code'])
            dataNew = data[['Street_Code', 'Pre_Direction', 'Street_Name', 'Post_Direction', 'City_Code', 'Added_Manually', 'Added_By', 'Added', 'Local_Name']]
            new_data = pd.concat([dfDefaultStreet, dataNew]).reset_index(drop=True)
            new_data.to_csv(filePath, index=False, float_format='%.0f')
        
    for s in dfSubstreets['StoreID'].unique():
        if isnan(s) == 0:
            storeDirectory = storeStreetDirectory + r"\\" + str(int(s)) + r"\\"
            filePath = storeDirectory + 'substreets.txt'
            isStoreDirExist = os.path.exists(storeDirectory)
            if not isStoreDirExist:
                os.makedirs(storeDirectory)
            data = dfSubstreets.loc[dfSubstreets['StoreID'] == s]
            dataNew = data[['Sequence_Number', 'Street_Code', 'Pri_Lower_Bound' , 'Pri_Upper_Bound', 'Pri_Odd_Even', 'Sec_Lower_Bound', 'Sec_Upper_Bound', 'Sec_Odd_Even', 'Car_Route', 'Zip4Lower_Bound', 'Zip4Upper_Bound', 'Location_Code', 'Postal_Code', 'LDA_Code', 'LDA_Added_By', 'LDA_Added', 'Added_Manually', 'Added_By', 'Added', 'Updated_By', 'Updated', 'Pri_Lower_Bound_Dec', 'Pri_Upper_Bound_Dec', 'Sec_Lower_Bound_Dec', 'Sec_Upper_Bound_Dec', 'Is_In_Delivery_Area']]
            df = pd.concat([dataNew, dfDefaultSubstreet]).reset_index(drop=True)
            df.to_csv(filePath, index=False, float_format='%.0f')
    
    print('Successfully create store streets.')
except Exception as error:
    print("An exception occurred:", error)
finally:
    print('Complete.')