
import streamlit as st
import pandas as pd
import numpy as np
from tabular_data import metrics_data
from plots import swarm_plot, count_plot, lineplot, box
import plotly.express as px

#Setting up the app parameteres.

st.set_page_config(
    page_title="Supermarket Data Analysis",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)


#Setting the Header of the app.

st.header("Supermarket Data Dashboard. 🛒") 


#Setting up the Menu

menu = ['About', 'Tabular Data', 'Plot Area']
choice = st.sidebar.selectbox("Menu", menu)

#Creating information about the App.

if choice == "About":
    st.subheader("General information about app. ✍️")
    st.write("The dashboard is about Exploratory Data Analysis connected with Data Visualisation.\
             We will be working on a Supermarket dataset that can be found [here](https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales).")
    

    st.write("On the left side, You have Menu sidebar. Pick whatever tab you want to \
             see the particular information.")
    img,img_2,img_3 = st.columns(3)
    with img:
        st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSrk55ZZK_QmM9Ig4reInwi2WvznPiLnMJ-PQ&usqp=CAU",\
                 width=600)

#Creating Tabular Data Information

elif choice == "Tabular Data":
    metrics_data()

#Creating the Plot tab.

else:
    swarm, scatter = st.columns(2)
    with swarm:
        swarm_plot()
    with scatter:
        count_plot()
    line, boxplot = st.columns(2)
    with line:
        lineplot()
    with boxplot:
        box()





