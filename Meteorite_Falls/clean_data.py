import pandas as pd 
import numpy as np
import re


def import_data(path):
    
    df = pd.read_csv(path)
    return df
    

def clean_data():
    data = import_data('Portfolio/Meteorite_Falls/Meteorite_Landings_20231031.csv')
    
    #Dealing with missing Mass (g) We will fill them with NaNMean values of the data.
    data['mass (g)'] = data['mass (g)'].fillna(np.nanmean(data['mass (g)']))
    
    #Dealing with year. All the Year with missing value will be replaced with value 600.
    
    data['year'] = data['year'].fillna(600)
    
    # We will also drop all of the data where, we dont have any information about Geolocation
    
    data = data[~data['GeoLocation'].isnull()]
    
    data['Mass (kg)'] = round(data['mass (g)'] / 1000,3)
    
    #Cleaning the Types of the Data
    
    data['year'] = data['year'].astype(int)
    
    
    #Capitalising the Data Columns
    for col in data.columns:
        col_cap = str(col).capitalize()
        data.rename(columns={col:col_cap},inplace=True)
    
    
    #Adding the City Column
    
    pattern = r'^[^\d]+'
    city_list = []
    for city in data['Name']:
        found = re.findall(pattern, city)
        city_list.append(''.join(found))
        
    data['City'] = city_list
        

    
    return data
    

clean_data()