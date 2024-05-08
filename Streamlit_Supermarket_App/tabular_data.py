import pandas as pd 
import streamlit as st
import numpy as np
from streamlit_extras.metric_cards import style_metric_cards
#Creating a function to import the data.

def import_clean_data():
    data = pd.read_csv('streamlit_app/super_market.csv')
    return data


#Creating a Function to calculate the Metrics.

def metrics_data(data = import_clean_data()):


    filter_column = st.selectbox("Pick a column to filter.", list(data[['City','Gender','Product line','Payment','Customer type']].columns) + ['None'])


    #Adding a If statement to see the totals, without the filters.

    if filter_column == "None":

        #First row of the Metrics
        unit_price, quantity, tot_price, rating = st.columns(4)

        #Calculating the Average values of Data.
        avg_price = data['Unit price'].mean()
        avg_quantity = data['Quantity'].mean()
        avg_total_price = data['Total'].mean()
        avg_rating = data['Rating'].mean()
        
        with unit_price:
            st.metric("Avg Cost of Product. 💰", str(round(avg_price,2))+"$")

        with quantity:
            st.metric("Avg units bought. 🛒", int(round(avg_quantity,0)))
    
        with tot_price:
            st.metric("Avg total money spent. 💵", str(round(avg_total_price,2))+"$")

        with rating:
            st.metric("Avg Rating.📈", round(avg_rating,2))

        #Second Row of Data Metrics

        total_quantity_count, total_gross_income, total_customers, most_popular_payment = st.columns(4)

        #Calculating the totals of Data.
        quantity_count = data['Quantity'].sum()
        total_earned = data['Total'].sum()
        total_gross = data['gross income'].sum()
        n_customers = data['City'].count()
        payment_pop = data['Payment'].value_counts().index[0]
        n_payment_pop = data['Payment'].value_counts()[0]

        with total_quantity_count:
            st.metric("Total goods sold. 📊", round(quantity_count,2))

        with total_gross_income:
            st.metric("Total gross Income. 💼", str(round(total_gross,2))+"$")

        with total_customers:
            st.metric("Total customers. 👪", n_customers)

        with most_popular_payment:
            st.metric("Most Popular way of Paying", str(payment_pop) +  "-" + str(n_payment_pop))

        col1, col2, col3 = st.columns(3)
        with col2:
            col2.metric("Total money Earned. 🤑", str(round(total_earned,2))+"$")
            style_metric_cards(background_color='#000000', border_color='#BF0909', box_shadow=False)
        st.subheader("Click on any column to sort the data.")
        st.dataframe(data)


    else:
        picked_value = st.selectbox("Pick a value.", data[filter_column].unique())
        unit_price, quantity, tot_price, rating = st.columns(4) # First row of the Data Metrics.

    # Averages of columns picked by user.

        avg_price = data[data[filter_column]==picked_value]['Unit price'].mean()
        avg_quantity = data[data[filter_column]==picked_value]['Quantity'].mean()
        avg_total_price = data[data[filter_column]==picked_value]['Total'].mean()
        avg_rating = data[data[filter_column]==picked_value]['Rating'].mean()

        with unit_price:
            st.metric("Avg Cost of Product. 💰", str(round(avg_price,2))+"$")

        with quantity:
            st.metric("Avg units bought. 🛒", int(round(avg_quantity,0)))
    
        with tot_price:
            st.metric("Avg total money spent. 💵", str(round(avg_total_price,2))+"$")

        with rating:
            st.metric("Avg Rating.📈", round(avg_rating,2))

    # Second row of Data Metrics.

        total_quantity_count, total_gross_income, total_customers, most_popular_payment = st.columns(4)

    # Totals and counts of Data.

        quantity_count = data[data[filter_column]==picked_value]['Quantity'].sum()
        total_earned = data[data[filter_column]==picked_value]['Total'].sum()
        total_gross = data[data[filter_column]==picked_value]['gross income'].sum()
        n_customers = data[data[filter_column]==picked_value]['City'].count()
        payment_pop = data[data[filter_column]==picked_value]['Payment'].value_counts().index[0]
        n_payment_pop = data[data[filter_column]==picked_value]['Payment'].value_counts()[0]
    

        with total_quantity_count:
            st.metric("Total goods sold. 📊", round(quantity_count,2))

        with total_gross_income:
            st.metric("Total gross Income. 💼", str(round(total_gross,2))+"$")

        with total_customers:
            st.metric("Total customers. 👪", n_customers)

        with most_popular_payment:
            st.metric("Most Popular way of Paying", str(payment_pop) +  "-" + str(n_payment_pop))

        col1, col2, col3 = st.columns(3)
        with col2:
            col2.metric("Total money Earned. 🤑", str(round(total_earned,2))+"$")
            style_metric_cards(background_color='#000000', border_color='#BF0909', box_shadow=False)
        st.subheader("Click on any column to sort the data.")
        st.dataframe(data[data[filter_column]==picked_value])   




