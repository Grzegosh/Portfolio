import streamlit as st
import pandas as pd
from clean_data import clean_data
from tabular_data import statistics
from plot_area import distribution_of_mass, lineplot, count_fall, chemistry_type_bar
import seaborn as sns
from map_data import display_data

#Setting up the overall parameters of the dashboard.

sns.set_style('whitegrid')




st.set_page_config(
    page_title="Meteorite Landings",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)



#Setting up the CSS Class

st.markdown("""
<style>
.font_class {
    font-size:24px;
    padding: 10px;
    border: 10px solid red;

}
</style>
""", unsafe_allow_html=True)



#Setting up the MENU

MENU = ['About','Information about Cleaned Data','Tabular Data and Statistics','Plot Area', 'Maps']

MENU_CHOICE = st.sidebar.selectbox("Control Panel", MENU)


#Organising the About page.

if MENU_CHOICE == "About":
    st.header("Meteorite Landings Dashboard.🧑‍🚀")
    st.subheader("What's inside this project?")
    
    st.write("<span style ='font-size:21px;'>In this project we will try to visualise\
        Meteorite landings in our planet since 860 till 2013. \
            \nWe will mostly focus on:</span>",unsafe_allow_html=True)
    
    st.markdown('<p class = "font_class">&#8226 Locations, </p>', unsafe_allow_html=True)
    st.markdown('<p class = "font_class">&#8226 Frequency, </p>', unsafe_allow_html=True)
    st.markdown('<p class = "font_class">&#8226 Descriptive Statistics, </p>', unsafe_allow_html=True)
    st.markdown('<p class = "font_class">&#8226 Class of the Meteorite. </p>', unsafe_allow_html=True)
    
    st.write("<span \
        style = 'font-size:21px;'>All the data\
        can be acquired from [here](https://data.nasa.gov/Space-Science/Meteorite-Landings/gh4g-9sfh)\
            \nOn the left,  you have control panel. Pick whatever tab you want to get the insights.\
            </span>"\
                ,unsafe_allow_html=True)
    
    st.write("<span style = 'font-size:21px; padding:500px'>\
        The general DataFrame  looks like this.</span>",unsafe_allow_html=True)
    
    st.dataframe(clean_data().head(),width=1500)

#Tabular Data and Statistics TAB 
    
elif MENU_CHOICE == "Tabular Data and Statistics":
    st.header("Tabular Data and Statistics 🧑‍🔬")
    statistics()
    
    

#Information about Cleaned Data TAB

elif MENU_CHOICE == "Information about Cleaned Data":
    st.header("Information about Missing Data ❎")
    st.write("In this particular dataframe we were having <span\
        style = 'color:red; font-size:24px;' >7737 </span> missing values.\
            Dividing them into columns we have:", unsafe_allow_html=True)
    
    st.markdown('<p class = "font_class">&#8226 131 empty values in Mass column - \
                filled with NaNMean value (13278.07 g). </p>', unsafe_allow_html=True)
    
    st.markdown('<p class = "font_class">&#8226 291 empty values in Year column - \
                filled with hard coded 600 (just for visualising the data). </p>', unsafe_allow_html=True)
    
    st.markdown('<p class = "font_class">&#8226 7315 empty values in GeoLocation column - \
                deleted from the dataframe. </p>', unsafe_allow_html=True)

    st.write("<span style = 'font-size:24px;' >In result we have information about 38401 Meteorite\
        Landings in our planet.", unsafe_allow_html=True)
    
    image_url = 'https://media.tenor.com/gTjstWRt7p8AAAAC/meteor-moonfall.gif'
    
    st.write(
    f'<div style="display: flex; justify-content: center;">'
    f'<img src="{image_url}" style="max-width: 40%; height: auto;" />'
    f'</div>',
    unsafe_allow_html=True,
)

# Plot Data

elif MENU_CHOICE == "Plot Area":
    st.header("Plot Area 📊")
    mass, line = st.columns(2)
    with mass:
        distribution_of_mass()
    with line:
        lineplot()
    count, chem = st.columns(2)
    with count:    
        count_fall()
    with chem:
        chemistry_type_bar()
        
# MAPS
elif MENU_CHOICE == "Maps":
    st.header("Geographic vision on Landing. 🌍")
    display_data()
