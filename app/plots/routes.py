from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
import plotly.graph_objects as go
from datetime import datetime
import requests
from app import db
from app.models import *
from app.plots import bp
import pandas as pd
import io
from app.api import vis

## Tests

def new_tests_plot():

    df = vis.get_testresults()

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New tests'].tail(1).values[0],))

    fig.add_trace(go.Scatter(x=df.Date,y=df['New tests'],marker_color='#5E5AA1',visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "New Tests"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New tests'].iloc[-2]}}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h",

        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "New Tests Over Time"},]),
            ]),
        )
    ])


    div = fig.to_json()
    p = Viz.query.filter_by(header="New Tests").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_tests_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Total tested'].notna()]


    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Total tested'].tail(1).values[0],))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Total tested'],marker_color='#5E5AA1', visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Total tested"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Total tested'].iloc[-2]}}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h",

        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Total tested Over Time"},]),
            ]),
        )
    ])


    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Tested").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def tested_positve_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['New Positive pct'].notna()]
    temp = df.loc[df['New Positive pct'] > 0]

    fig.add_trace(go.Indicator(
        mode = "number",
        value = df['New Positive pct'].tail(1).values[0]*100,))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['New Positive pct'],marker_color='#5E5AA1',visible=False))


    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "New Positive %"},
            'mode' : "number"}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h",

        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Positive % Over Time"},]),
            ]),
        )
    ])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Tested Positive").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def under_investigation_plot():
    df = vis.get_testresults()
    fig = go.Figure()
    temp = df.loc[df['Total tested'].notna()]

    fig.add_trace(go.Indicator(
        mode = "number",
        value = df['Under Investigation'].tail(1).values[0],))


    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Under Investigation'],marker_color='#5E5AA1', visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Under Investigation"},
            'mode' : "number"}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h",

        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Under Investigation Over Time"},]),
            ]),
        )
    ])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Under Investigation").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Hospital
def in_hospital_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Hospitalized'].notna()]
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = temp['Hospitalized'].tail(1).values[0],))
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Hospitalized'],marker_color='#54CAF1',visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "In Hospital"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Hospitalized'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E4F7FD',
        paper_bgcolor="#E4F7FD",
        legend_orientation="h",
        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Hospitalized Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="In Hospital").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def in_icu_plot():

    df = vis.get_testresults()
    fig = go.Figure()
    temp = df.loc[df['ICU'].notna()]
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = temp['ICU'].tail(1).values[0],))
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU'],marker_color='#54CAF1',visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "In ICU"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['ICU'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E4F7FD',
        paper_bgcolor="#E4F7FD",
        legend_orientation="h",
        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "In ICU Over Time"},]),
            ]),
        )])
    div = fig.to_json()
    p = Viz.query.filter_by(header="In ICU").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def on_ventilator_plot():

    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Ventilator'].notna()]
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = temp['Ventilator'].tail(1).values[0],))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Ventilator'],marker_color='#54CAF1',visible=False))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "On Ventilator"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Ventilator'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E4F7FD',
        paper_bgcolor="#E4F7FD",
        legend_orientation="h",
        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "On Ventilator Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="On Ventilator").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Cases

def total_cases_plot():

    df = vis.get_testresults()

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Positives'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Total cases"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Positives'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Positives'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Cases Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_cases_plot():
    df = vis.get_testresults()

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New positives'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "New cases"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New positives'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['New positives'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "New Cases Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="New Cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def recovered_plot():
    df = vis.get_testresults()

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Resolved'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Recovered Cases"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Resolved'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Resolved'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Recovered Cases Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Recovered").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_deaths_plot():
    df = vis.get_testresults()
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Deaths'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Total Deaths"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Deaths'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Deaths Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Deaths").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_deaths_plot():
    df = vis.get_testresults()
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Deaths'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Total Deaths"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Deaths'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Deaths Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Deaths").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_deaths_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New deaths'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "New Deaths"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New deaths'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['New deaths'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "New deaths Over Time"},]),
            ]),
        )])
    div = fig.to_json()
    p = Viz.query.filter_by(header="New Deaths").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Regional

def cases_region_plot():
    PHU = {
        "Erie St. Clair": ['Chatham-Kent Health Unit', 'Lambton Health Unit', 'Windsor-Essex County Health Unit'],
        "South West":['Middlesex-London Health Unit', 'Grey Bruce Health Unit', 'Haldimand-Norfolk','Southwestern','Huron County Health Unit'],
        "Waterloo Wellington":['Wellington-Dufferin-Guelph Health Unit','Grey Bruce Health Unit','Waterloo Health Unit'],
        "Hamilton Niagara Haldimand Brant":['Brant County Health Unit','City of Hamilton Health Unit','Halton Regional Health Unit','Haldimand-Norfolk','Niagara Regional Area Health Unit'],
        "Central West":['Wellington-Dufferin-Guelph Health Unit','Toronto Public Health'],
        "Mississauga Halton":['Peel Regional Health Unit','Halton Regional Health Unit','Toronto Public Health'],
        "Toronto Central":['City of Toronto Health Unit'],
        "Central":['York Regional Health Unit','Toronto Public Health'],
        "Central East":['Peterborough County-City Health Unit','Haliburton, Kawartha','Pine Ridge District Health Unit','Toronto Public Health','Durham Regional Health Unit'],
        "South East":['Hastings and Prince Edward Counties Health Unit','Leeds, Grenville and Lanark District Health Unit','Kingston, Frontenac, and Lennox and Addington Health Unit','Haliburton, Kawartha, Pine Ridge District Health Unit'],
        "Champlain":['Leeds Grenville and Lanark','The Eastern Ontario Health Unit','City of Ottawa Health Unit','Renfrew'],
        "North Simcoe Muskoka":['Simcoe Muskoka District Health Unit','Grey Bruce Health Unit'],
        "North East":['Northwestern Health Unit','Timiskaming Health Unit','North Bay Parry Sound District Health Unit','The District of Algoma Health Unit','Sudbury and District Health Unit','Porcupine Health Unit'],
        "North West":['Northwestern Health Unit','Thunder Bay']
    }

    Regions = {
        "West":["Erie St. Clair","South West","Waterloo Wellington","Hamilton Niagara Haldimand Brant"],
        "Central":["Central West","Mississauga Halton","Central","North Simcoe Muskoka"],
        "Toronto":["Toronto Central"],
        "East": ["Central East","South East","Champlain"],
        "North": ["North East","North West"]
    }


    df = vis.get_phus()
    for item in PHU:
        df.loc[df.region.isin(PHU[item]),'LHIN'] = item

    for item in Regions:
        df.loc[df.LHIN.isin(Regions[item]),'Region'] = item



    df = df.drop(df.loc[df.region == 'Ontario'].index)

    regions = df.region.values

    df.region = [thing.replace("Health Unit","") for thing in regions]

    fig = go.Figure()

    df_t = df.groupby('region')['value'].sum().sort_values()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h', text=df_t.values, textposition='inside',marker_color="#413C90"))

    df_t = df.groupby('LHIN')['value'].sum().sort_values()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h',text=df_t.values, textposition='inside',visible=False,marker_color="#413C90"))

    df_t = df.groupby('Region')['value'].sum().sort_values()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h',text=df_t.values, textposition='inside',visible=False, marker_color="#413C90"))

    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Cases by Public Health Unit",
                'y':0.98,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        ),
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="PHU",
                     method="update",
                     args=[{"visible": [True, False, False]},
                           {"title": "Total Cases by Public Health Unit"}]),
                dict(label="LHIN",
                     method="update",
                     args=[{"visible": [False, True, False]},
                           {"title": "Total Cases by LHIN"}]),
                dict(label="Region",
                     method="update",
                     args=[{"visible": [False, False, True]},
                           {"title": "Total Cases by LHIN Region"}]),
            ]),
        )
    ])

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
    )
    div = fig.to_json()
    p = Viz.query.filter_by(header="Cases by Region").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def residual_table_plot():
    df = vis.get_icu_capacity()
    last_date = df.tail(1).date.values[0]
    df = df.loc[df.date == last_date]


    fig = go.Figure()

    fig.add_trace(
        go.Table(
            header=dict(
                values=["LHIN", "Residual Beds", "Residual Ventilators"],
                font=dict(size=10),
                align="left",
                fill_color='#E0DFED'
            ),
            cells=dict(
                values=[df[k].tolist() for k in df[['lhin', 'residual_beds', 'residual_ventilators']].columns[:]],
                align = "left",
            fill_color='#E0DFED')
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Residual Table").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def lhin_icu_plot():
    df = vis.get_icu_capacity()

    last_date = df.tail(1).date.values[0]
    df = df.loc[df.date == last_date]

    df.rename(columns={"confirmed_positive":"Confirmed Covid"},inplace=True)
    df.rename(columns={"suspected_covid":"Suspected Covid"},inplace=True)
    df.rename(columns={"non_covid":"Not Covid"},inplace=True)

    fig = go.Figure()

    for thing in ['Confirmed Covid','Suspected Covid','Not Covid']:
        fig.add_trace(go.Bar(name=thing,y=df.lhin, x=df[thing],orientation='h'))

    fig.update_layout(barmode='stack')

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h"
    )

    fig.update_layout(barmode='stack')
    div = fig.to_json()
    p = Viz.query.filter_by(header="LHIN ICU Breakdown").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Mobility

def toronto_mobility_plot():
    df = vis.get_mobility_transportation()

    df = df.loc[df.region=='Toronto']
    fig = go.Figure()

    ttype = df.transportation_type.unique()

    for thing in ttype:
        temp = df.loc[df.transportation_type == thing]
        fig.add_trace(go.Scatter(name=thing,x=temp.date,y=temp['value']))

    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"Toronto Mobility",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )


    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Toronto Mobility").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def retail_mobility_plot():
    df = vis.get_mobility()
    df = df.loc[df.region == 'Ontario']
    df = df.sort_values(['region', 'category', 'date'])

    fig = go.Figure()

    ttype = df.category.unique()

    fig.add_trace(go.Scatter(name=ttype[0],x=df.loc[df.category == ttype[0]].date,y=df.loc[df.category == ttype[0]]['value']))

    for item in ttype[1:]:
        fig.add_trace(go.Scatter(name=item,x=df.loc[df.category == item].date,y=df.loc[df.category == item]['value'],visible=False))


    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"{ttype[0]} {int(df.loc[df.category == ttype[0]]['value'].tail(1))}%",
                'y':0.99,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        ),
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="Grocery",
                     method="update",
                     args=[{"visible": [True, False, False, False, False, False]},
                           {"title": f"{ttype[0]} {int(df.loc[df.category == ttype[0]]['value'].tail(1))}%"}]),
                dict(label="Parks",
                     method="update",
                     args=[{"visible": [False, True, False, False, False, False]},
                           {"title": f"{ttype[1]} {int(df.loc[df.category == ttype[1]]['value'].tail(1))}%"}]),
                dict(label="Residential",
                     method="update",
                     args=[{"visible": [False, False, True, False, False, False]},
                           {"title": f"{ttype[2]} {int(df.loc[df.category == ttype[2]]['value'].tail(1))}%"}]),
                dict(label="Retail",
                     method="update",
                     args=[{"visible": [False, False, False, True, False, False]},
                           {"title": f"{ttype[3]} {int(df.loc[df.category == ttype[3]]['value'].tail(1))}%"}]),

                dict(label="Transit",
                     method="update",
                     args=[{"visible": [False, False, False, False, True, False]},
                           {"title": f"{ttype[4]} {int(df.loc[df.category == ttype[4]]['value'].tail(1))}%"}]),

                dict(label="Work",
                     method="update",
                     args=[{"visible": [False, False, False, False, False, True]},
                           {"title": f"{ttype[5]} {int(df.loc[df.category == ttype[5]]['value'].tail(1))}%"}]),
            ]),
        )
    ])

    fig.update_layout(
        margin=dict(l=0, r=20, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Retail Mobility").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Capacity
def icu_ontario_plot():
    df = vis.get_icu_capacity_province()

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_beds'].tail(1).values[0],
    ),
                 )

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "ICU Beds Left"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['residual_beds'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_beds'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Residual Beds Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="ICU Ontario").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ventilator_ontario_plot():
    df = vis.get_icu_capacity_province()
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_ventilators'].tail(1).values[0],
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title': {'text': "Ventilators Left"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['residual_ventilators'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_ventilators'],marker_color='#497787', visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=1,
            y=-0.1,
            buttons=list([
                dict(label="KPI",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": ""}
                          ]),
                dict(label="Trend",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Residual Ventilators Over Time"},]),
            ]),
        )])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Ventilator Ontario").first()
    p.html = div
    db.session.add(p)
    db.session.commit()
    return

def icu_projections_plot():
    url = "https://docs.google.com/spreadsheets/d/11y3g_qr05H6u6gDorZKUsJKWLoq6KerOfhPYOsnI55Q/export?format=csv&id=11y3g_qr05H6u6gDorZKUsJKWLoq6KerOfhPYOsnI55Q&gid=0"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'])

    dft = vis.get_icu_case_status_province()
    dft.case_status.replace("suspected_covid","Suspected Covid",inplace=True)
    dft.case_status.replace("confirmed_positive","Confirmed Covid",inplace=True)

    fig = go.Figure()

    types = dft.case_status.unique()

    for thing in types:
        temp = dft.loc[dft.case_status==thing]
        fig.add_trace(go.Bar(name=thing,x=temp.date, y=temp.number))

    fig.add_trace(go.Scatter(name='Worst Case',x=df.Date,y=df['Worst Case'],opacity=.3,marker_color='red'))
    fig.add_trace(go.Scatter(name='Best Case',x=df.Date,y=df['Best Case'],opacity=.3,marker_color='blue'))

    fig.update_layout(barmode='stack')

    fig.update_layout(
        yaxis =  {'gridcolor': '#d1cfc8', 'title': 'Confirmed Covid Cases In ICU'},
        xaxis = {'showgrid': False},
        title={'text':f"COVID ICU Cases",
                'y':1,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E4F7FD',
        paper_bgcolor="#E4F7FD",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="ICU Projections").first()
    p.html = div
    db.session.add(p)
    db.session.commit()
    return

## Socioeconomic

def socio_plot():
    url = "https://docs.google.com/spreadsheets/d/1PTVSFZwSVWldmDpFEO_xXxTB-xSFtGq3pDPUKeLDEec/export?format=csv&id=1PTVSFZwSVWldmDpFEO_xXxTB-xSFtGq3pDPUKeLDEec&gid=0"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))

    df.SES.replace(1,"1 (least deprived)",inplace=True)
    df.SES.replace(5,"5 (most deprived)",inplace=True)


    fig = go.Figure()

    types = df['case status'].unique()

    socio = ["1 (least deprived)",2,3,4,"5 (most deprived)"]

    colors = ['blue', 'red']

    colors = ['#B9DDF1','#8BBADC','#6798C1','#4776A4','#2A5783']

    for ttype, color in zip(socio,colors):
        temp = df.loc[df.SES == ttype]
        fig.add_trace(go.Bar(name=ttype,x=types,y=temp['value'].values,text=temp['value'].values,textposition='auto',marker_color=color))


    fig.update_layout(barmode='stack')

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h"
    )

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': True,'visible':True},
        title={'text':f"Distribution of COVID19 Cases by Neighbourhood Socioeconmic Status",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Socioeconmic").first()
    p.html = div
    db.session.add(p)
    db.session.commit()
    return


## Trajectory
def canada_cases_plot():
    df = vis.get_cases_rolling_average()
    Canada = ['Ontario', 'BC', 'Alberta', 'Quebec', 'Saskatchewan',
       'Nova Scotia', 'NL', 'New Brunswick', 'Manitoba', 'Canada']
    International = ['Ontario','Canada',
           'Italy', 'Korea, South', 'Spain', 'United Kingdom', 'France', 'US']

    fig = go.Figure()

    for item in Canada:
        temp = df.loc[df.region == item]
        fig.add_trace(go.Scatter(name=item,x=temp.date_shifted,y=temp.average))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': True,'visible':True, 'type': 'log'},
        title={'text':f"Provincial Comparison: COVID19 Reported Cases Per Day",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Canada Cases Average").first()
    p.html = div
    db.session.add(p)
    db.session.commit()


    return

def international_cases_plot():
    International = ['Ontario','Canada',
       'Italy', 'Korea, South', 'Spain', 'United Kingdom', 'France', 'US']

    df = vis.get_cases_rolling_average()

    fig = go.Figure()

    for item in International:
        temp = df.loc[df.region == item]
        fig.add_trace(go.Scatter(name=item,x=temp.date_shifted,y=temp.average))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': True,'visible':True, 'type': 'log'},
        title={'text':f"International Comparison: COVID19 Reported Cases Per Day",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="International Cases Average").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def canada_deaths_plot():
    df = vis.get_deaths_rolling_average()
    Canada = ['Ontario', 'BC', 'Alberta', 'Quebec', 'Saskatchewan',
       'Nova Scotia', 'NL', 'New Brunswick', 'Manitoba', 'Canada']

    fig = go.Figure()

    for item in Canada:
        temp = df.loc[df.region == item]
        fig.add_trace(go.Scatter(name=item,x=temp.date_shifted,y=temp.average))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': True,'visible':True, 'type': 'log'},
        title={'text':f"Provincial Comparison: COVID19 Reported Deaths Per Day",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#DFE7EA',
        paper_bgcolor="#DFE7EA",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Canada Deaths Average").first()
    p.html = div
    db.session.add(p)
    db.session.commit()


    return

def international_deaths_plot():
    International = ['Ontario','Canada',
       'Italy', 'Korea, South', 'Spain', 'United Kingdom', 'France', 'US']

    df = vis.get_deaths_rolling_average()
    fig = go.Figure()

    for item in International:
        temp = df.loc[df.region == item]
        fig.add_trace(go.Scatter(name=item,x=temp.date_shifted,y=temp.average))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': True,'visible':True, 'type': 'log'},
        title={'text':f"International Comparison: COVID19 Reported Deaths Per Day",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="International Deaths Average").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ontario_death_plots():
    df = vis.get_daily_deaths()

    df = df.loc[df.region == 'Ontario']

    fig = go.Figure()

    fig.add_trace(go.Bar(x=df.date,y=df.daily_deaths,text=df.daily_deaths,textposition='auto', marker_color='#4F4B99'))

    fig.update_layout(
        title={'text':"Daily Deaths in Ontario: Comparison of COVID-19 and Estimates of non-COVID-19 Leading Causes",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=40, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        showlegend=False
    )

    fig.add_shape(
            # Line Horizontal
                type="line",
                x0=df.date.head(1).values[0],
                y0=8,
                x1=df.date.tail(1).values[0],
                y1=8,
                line=dict(
                    color="grey",
                    width=1,
                ),
        )

    fig.add_shape(
            # Line Horizontal
                type="line",
                x0=df.date.head(1).values[0],
                y0=14,
                x1=df.date.tail(1).values[0],
                y1=14,
                line=dict(
                    color="grey",
                    width=1,
                ),
        )

    fig.add_shape(
            # Line Horizontal
                type="line",
                x0=df.date.head(1).values[0],
                y0=16,
                x1=df.date.tail(1).values[0],
                y1=16,
                line=dict(
                    color="grey",
                    width=1,
                ),
        )

    fig.add_shape(
            # Line Horizontal
                type="line",
                x0=df.date.head(1).values[0],
                y0=56,
                x1=df.date.tail(1).values[0],
                y1=56,
                line=dict(
                    color="grey",
                    width=1,
                ),
        )

    fig.add_shape(
            # Line Horizontal
                type="line",
                x0=df.date.head(1).values[0],
                y0=83,
                x1=df.date.tail(1).values[0],
                y1=83,
                line=dict(
                    color="grey",
                    width=1,
                ),
        )

    fig.update_shapes(dict(xref='x', yref='y'))

    fig.add_trace(go.Scatter(
        x=[df.date.head(3).values[1], df.date.head(3).values[1], df.date.head(3).values[1],df.date.head(3).values[1],df.date.head(3).values[1],df.date.head(3).values[1]],
        y=[8+1,14+1,16+1,56+1,83+1],
        text=["Diabetes","Cerebrovascular Disease","Accidents / Injuries","Heart Disease","All Cancers"],
        mode="text",
    ))

    div = fig.to_json()
    p = Viz.query.filter_by(header="Ontario Death Comparison").first()
    p.html = div
    db.session.add(p)
    db.session.commit()


    return
