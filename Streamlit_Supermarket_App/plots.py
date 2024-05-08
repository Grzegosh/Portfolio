import plotly.express as px
import pandas as pd 
import numpy as np
from tabular_data import import_clean_data
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt


#Creating the Swarm plot of our data

def swarm_plot(data = import_clean_data()):
    sns.set_style('whitegrid')
    columns_to_drop = ['Invoice ID', 'Date', 'Time','gross margin percentage']
    data_frame = data.drop(columns_to_drop, axis=1)
    x_column = [column for column in data_frame.columns if data_frame[column].dtype != 'float64' or data_frame[column].dtype !='int64']
    y_column = [column for column in data_frame.columns if data_frame[column].dtype == 'float64' or data_frame[column].dtype == 'float64']


    x_selection = st.selectbox("Pick your X-axis", x_column)
    y_selection = st.selectbox("Pick your Y-axis.", y_column)
    color = [column for column in data_frame.columns if column not in x_selection and column not in y_column]
    color_selection = st.selectbox("Pick your hue.", color)

    fig = plt.figure(figsize=(12,6))
    sns.swarmplot(data=data_frame, x=x_selection, y=y_selection, hue=color_selection)
    plt.title(f"{x_selection} distribution based on {y_selection} and {color_selection}")
    st.pyplot(fig)

#Countplot using Plotly Express

def count_plot(data = import_clean_data()):
    columns_to_drop = ['Invoice ID', 'Date', 'Time','gross margin percentage']
    data = data.drop(columns_to_drop,axis=1)
    x_column = [column for column in data.columns if data[column].dtype != 'float64' or data[column].dtype !='int64']
    x_selection = st.selectbox("Select your X-axis.", x_column)
    hue_column = [column for column in data.columns if column not in x_selection and data[column].dtype != 'float64' and data[column].dtype!='int64']
    color_selection = st.selectbox("Select your legend.", hue_column)
    order_cat = data[x_selection].value_counts().index

    count = px.histogram(
        data_frame=data,
        x=x_selection,
        color=color_selection,
        title=f"Number of {x_selection} sliced by {color_selection}",
        text_auto=True
    )
    count.update_traces(textposition = "inside")
    st.plotly_chart(count)


#Creating a Lineplot of our Data.

def lineplot(data = import_clean_data()):
    data['Date'] = pd.to_datetime(data['Date'])
    grouped_data = pd.DataFrame(data.groupby(['Date','Gender']).agg({'Total':'sum','Quantity':'sum'}).reset_index())
    available_columns = ['Total','Quantity']
    groupping_level = ['None','City','Gender','Product line']
    groupping_choice = st.radio("Select groupping level.", groupping_level)
    choice = st.selectbox("Select your summary column.", available_columns)
    if groupping_choice == 'None':
        grouped_data = pd.DataFrame(data.groupby('Date').agg({'Total':'sum','Quantity':'sum'})).reset_index()
        line = px.line(
            data_frame=grouped_data,
            x='Date',
            y=choice,
            title=f"Summary of {choice} over time.",
            width=590
        )
        st.plotly_chart(line)
    else:
        grouped_data = pd.DataFrame(data.groupby(['Date',groupping_choice]).agg({'Total':'sum','Quantity':'sum'})).reset_index()
        line = px.line(
            data_frame=grouped_data,
            x='Date',
            y=choice,
            color=groupping_choice,
            title=f"Summary of {choice} based on {groupping_choice}",
            width=591

        )
        st.plotly_chart(line)

def box(data=import_clean_data()):
    available_columns = ['Total','Quantity']
    x_axis = ['City','Customer type','Gender','Payment','Product line']
    x_choice = st.radio("Select your X-axis.", x_axis)
    summary_choice = st.selectbox("Select your summary column.", available_columns,key='2')

    box_plot = px.box(
        data_frame=data,
        x=x_choice,
        y=summary_choice,
        title=f"{summary_choice} based on {x_choice}"
    )

    st.plotly_chart(box_plot)








    

    



