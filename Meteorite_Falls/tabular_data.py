import pandas as pd 
import numpy as np
from clean_data import clean_data
import streamlit as st






def statistics():
    data = clean_data()
    cols_to_filter = ['City','Name','Recclass','Fall','Year']
    choices = st.multiselect("Which columns do you want to filter?: ", cols_to_filter)
    num_choices = len(choices)
    if num_choices == 0 :
        st.dataframe(data,use_container_width=True)
        mass_g_desc = pd.DataFrame(data['Mass (g)'].describe().T)
        mass_kg_desc = pd.DataFrame(data['Mass (kg)'].describe().T)
        nt_fall_desc = pd.DataFrame(data.groupby(['Nametype','Fall'])['Fall'].count())\
            .rename(columns={'Fall':'No of Hits'}).reset_index().sort_values(by='No of Hits',ascending=False)
            
        year_desc = pd.DataFrame(data.groupby('Year')['Year'].count())\
            .rename(columns={'Year':'No of Findings'}).reset_index().sort_values('No of Findings',ascending=False)\
                .set_index('Year')
        
        city_desc = pd.DataFrame(data['City'].value_counts())\
            .rename(columns={'City':'No of Hits'})
        
        mass_g, mass_kg, nt_fall, city, year = st.columns(5)
        
        with mass_g:
            st.write("<span style = 'font-size:24px;'>Descriptive statistics of Mass in Grams.</span>",unsafe_allow_html=True)
            st.dataframe(mass_g_desc, use_container_width=True)
        with mass_kg:
            st.write("<span style = 'font-size:24px;'>Descriptive statistics of Mass in KiloGrams.</span>",unsafe_allow_html=True)
            st.dataframe(mass_kg_desc, use_container_width=True)
            
        with nt_fall:
            st.write("<span style = 'font-size:24px;'>Descriptive statistics of Mass in KiloGrams.</span>",unsafe_allow_html=True)
            st.dataframe(nt_fall_desc, use_container_width=True)
            
        with city:
            st.write("<span style = 'font-size:24px;'>Number of recorded landings in Cities.</span>",unsafe_allow_html=True)
            st.dataframe(city_desc, use_container_width=True)
            
        with year:
            st.write("<span style = 'font-size:24px;'>Number of Findings and Hits by Years.</span>",unsafe_allow_html=True)
            st.dataframe(year_desc)
            
        
            
            
        st.subheader("Bullet Points: 🚄")
        st.write('<p class="font_class">&#8226 From overall <span class="red-text">3421</span> cities, \
        <span class="red-text">11</span> of them was responsible for \
        <span class="red-text">25486</span> landings out of <span class="red-text">38401</span>. \
            Which gives us about <span class="red-text">66,4%</span> from all of the hits.</p>', \
                unsafe_allow_html=True)

        st.write("""
        <style>
        .red-text {
            color: red;
        }
        </style>
        """, unsafe_allow_html=True)

        
        st.write('<p class="font_class">&#8226 The biggest Metorite weighted <span class="red-text">60 000 KG\
            </span></p>', unsafe_allow_html=True)
        
        
        st.write('<p class="font_class">&#8226 In <span class="red-text">1979</span> , \
        we found a meteorite  \
        <span class="red-text">3046</span> times, which is the biggest amount in human history. </p>', \
                unsafe_allow_html=True)
        
        st.write('<p class="font_class">&#8226 Skewness is equal to  <span class="red-text">70.61</span> , \
        which means, most of our meteorites weighted less than the mean weight.  \
        </p>', \
                unsafe_allow_html=True)
        

    else:
        data_filtered = data.copy()
        data_filtered = data_filtered.drop(['Reclat','Reclong','Geolocation'],axis=1)
        for item in choices:
            
            choice = st.multiselect(f"Pick a {item}", data_filtered[item].unique())
            data_filtered = data_filtered[data_filtered[item].isin(choice)]
        st.subheader("General Information About filtered Data.")
        st.dataframe(data_filtered,use_container_width=True)
            
        choises_picked = list(choices)
        st.write("<span style = 'font-size:24px;'>Descriptive statistics of mass in KG after filtering.</span>",unsafe_allow_html=True)
        groupped_data = data_filtered.groupby(choises_picked)['Mass (kg)'].describe()
        skewness = data_filtered.groupby(choises_picked)['Mass (kg)'].skew()
        var_coef_mean = data_filtered.groupby(choises_picked)['Mass (kg)'].mean()
        var_coef_std = data_filtered.groupby(choises_picked)['Mass (kg)'].std()
        V = round(var_coef_std / var_coef_mean) * 100
        groupped_data['Skewness'] = skewness
        groupped_data['Var. Coef'] = V
        st.dataframe(groupped_data,use_container_width=True)
  
    
            
            
        

            
        
    








    


    

    