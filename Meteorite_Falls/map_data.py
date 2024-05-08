from clean_data import clean_data
import streamlit as st

def display_data():
    data= clean_data()
    data.rename(columns={'Reclat':'LAT','Reclong':'LON'},inplace=True)
    st.map(data= data, latitude=data['LAT'], longitude=data['LON'],use_container_width=True,zoom=0)
    


    
    



