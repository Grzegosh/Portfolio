from clean_data import clean_data
import streamlit as st
import plotly.graph_objects as go




def distribution_of_mass():
    data = clean_data()
    bins = [0, 10, 100, 1000, 10000, 60000]
    
    figure = go.Figure(go.Histogram(
        x=data['Mass (kg)'],
        xbins=dict(
            start=min(bins),
            end=max(bins),
            size=1000,
        )
    ))
    figure.update_layout(
        title="General Distribution of Mass in (Kg) 🔍",
        xaxis_title="Mass (kg)",
        yaxis_title="Number of Observations",
        xaxis = dict(
            tickmode = 'array',
            tickvals = list(range(0, max(bins),5000))
        ),
        yaxis = dict(
            type = 'log'
        )
    )
    
    st.plotly_chart(figure)
    
    
    # Lineplot Function
    
def lineplot():
    data = clean_data()
    data_grouped = data.groupby('Year').size().reset_index(name='Count')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data_grouped['Year'], y=data_grouped['Count'], mode='lines'))
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Number of Observations",
        title = "Number of Landings by Years ☄️"
    )
    
    st.plotly_chart(fig)

def count_fall():
    data = clean_data()
    color_mapping = {
        'Fell':'blue',
        'Found':'red'
    }
    count_data = data['Fall'].value_counts().reset_index()
    count_data.columns = ['Category','Count']
    count_data['Color'] = count_data['Category'].map(color_mapping)
    
    fig = go.Figure(go.Bar(
        x=count_data['Category'],
        y=count_data['Count']
    ))
    
    fig.update_layout(
        title = "Number of Objects by type of Meteor 👇",
        xaxis_title = "Type of Meteor",
        yaxis_title = "Number of Observations"
    )
    st.plotly_chart(fig)
    

def chemistry_type_bar():
    data = clean_data()
    count_data = data['Recclass'].value_counts().reset_index()
    count_data.columns = ['Chem','Count']
    
    figure = go.Figure(go.Bar(
        x=count_data['Chem'],
        y=count_data['Count']
    ))
    figure.update_layout(
        title = "Number of Meteors by Chemistry type 👩‍🔬",
        xaxis_title = 'Chemisty Type',
        yaxis_title = "Number of Observations",
        yaxis = dict(
            type='log'
        )
    )
    st.plotly_chart(figure)


    

    
    









    