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
from sqlalchemy import sql

PHU = {'the_district_of_algoma':'The District of Algoma Health Unit',
 'brant_county':'Brant County Health Unit',
 'durham_regional':'Durham Regional Health Unit',
 'grey_bruce':'Grey Bruce Health Unit',
 'haldimand_norfolk':'Haldimand-Norfolk Health Unit',
 'haliburton_kawartha_pine_ridge_district':'Haliburton, Kawartha, Pine Ridge District Health Unit',
 'halton_regional':'Halton Regional Health Unit',
 'city_of_hamilton':'City of Hamilton Health Unit',
 'hastings_and_prince_edward_counties':'Hastings and Prince Edward Counties Health Unit',
 'huron_county':'Huron County Health Unit',
 'chatham_kent':'Chatham-Kent Health Unit',
 'kingston_frontenac_and_lennox_and_addington':'Kingston, Frontenac, and Lennox and Addington Health Unit',
 'lambton':'Lambton Health Unit',
 'leeds_grenville_and_lanark_district':'Leeds, Grenville and Lanark District Health Unit',
 'middlesex_london':'Middlesex-London Health Unit',
 'niagara_regional_area':'Niagara Regional Area Health Unit',
 'north_bay_parry_sound_district':'North Bay Parry Sound District Health Unit',
 'northwestern':'Northwestern Health Unit',
 'city_of_ottawa':'City of Ottawa Health Unit',
 'peel_regional':'Peel Regional Health Unit',
 'perth_district':'Perth District Health Unit',
 'peterborough_county_city':'Peterborough County–City Health Unit',
 'porcupine':'Porcupine Health Unit',
 'renfrew_county_and_district':'Renfrew County and District Health Unit',
 'the_eastern_ontario':'The Eastern Ontario Health Unit',
 'simcoe_muskoka_district':'Simcoe Muskoka District Health Unit',
 'sudbury_and_district':'Sudbury and District Health Unit',
 'thunder_bay_district':'Thunder Bay District Health Unit',
 'timiskaming':'Timiskaming Health Unit',
 'waterloo':'Waterloo Health Unit',
 'wellington_dufferin_guelph':'Wellington-Dufferin-Guelph Health Unit',
 'windsor_essex_county':'Windsor-Essex County Health Unit',
 'york_regional':'York Regional Health Unit',
 'southwestern':'Southwestern Public Health Unit',
 'city_of_toronto':'City of Toronto Health Unit'}


## Tests

def new_tests_plot():

    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New tests'].tail(1).values[0],number = {'font': {'size': 60}},))

    fig.add_trace(go.Scatter(x=df.Date,y=df['New tests'],marker_color='#5E5AA1',visible=True, opacity=0.5))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"New Tests<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New tests'].iloc[-2]}}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
)


    div = fig.to_json()
    p = Viz.query.filter_by(header="new tests").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_tests_plot():
    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()
    temp = df.loc[df['Total tested'].notna()]


    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Total tested'].tail(1).values[0],number = {'font': {'size': 60}},))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Total tested'],marker_color='#5E5AA1', visible=True, opacity=0.5))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"Total Tested<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Total tested'].iloc[-2]}}]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
)


    div = fig.to_json()
    p = Viz.query.filter_by(header="tests").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def tested_positve_plot():
    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()
    temp = df.loc[df['New Positive pct'].notna()]
    temp = df.loc[df['New Positive pct'] > 0]

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New Positive pct'].tail(1).values[0]*100,
        number = {'font': {'size': 60}},))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['New Positive pct'],marker_color='#5E5AA1',visible=True, opacity=0.5))


    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"New Positive %<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New Positive pct'].iloc[-2]*100,
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
)

    div = fig.to_json()
    p = Viz.query.filter_by(header="tested positive").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def under_investigation_plot():
    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()
    temp = df.loc[df['Total tested'].notna()]

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Under Investigation'].tail(1).values[0],number = {'font': {'size': 60}},))


    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Under Investigation'],marker_color='#5E5AA1', visible=True, opacity=0.5))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"Under Investigation<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Under Investigation'].iloc[-2],
                      'increasing': {'color':'grey'},
                      'decreasing': {'color':'grey'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
        legend_orientation="h",)

    div = fig.to_json()
    p = Viz.query.filter_by(header="under investigation").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Hospital
def in_hospital_plot():
    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()
    temp = df.loc[df['Hospitalized'].notna()]
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = temp['Hospitalized'].tail(1).values[0],number = {'font': {'size': 60}},))
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Hospitalized'],marker_color='#54CAF1',visible=True, opacity=0.5))

    fig.update_layout(
        template = {'data' : {'indicator': [{
        'title' : {"text": f"In Hospital<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Hospitalized'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
)

    div = fig.to_json()
    p = Viz.query.filter_by(header="in hospital").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def in_icu_plot(region='ontario'):

    if region=='ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()
        temp = df.loc[df['ICU'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['ICU'].tail(1).values[0],number = {'font': {'size': 60}},))
        fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU'],marker_color='#54CAF1',visible=True, opacity=0.5))

        fig.update_layout(
            template = {'data' : {'indicator': [{
            'title' : {"text": f"In ICU<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['ICU'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
            legend_orientation="h",)
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])

        fig = go.Figure()
        temp = df.loc[df['confirmed_positive'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['confirmed_positive'].tail(1).values[0],number = {'font': {'size': 60}},))

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive'],marker_color='#54CAF1',visible=True, opacity=0.5))

        fig.update_layout(
            template = {'data' : {'indicator': [{
            'title' : {"text": f"In ICU<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['confirmed_positive'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="in icu", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def on_ventilator_plot(region='ontario'):

    if region=='ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()
        temp = df.loc[df['Ventilator'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['Ventilator'].tail(1).values[0],number = {'font': {'size': 60}},))

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['Ventilator'],marker_color='#54CAF1',visible=True, opacity=0.5))

        fig.update_layout(
            template = {'data' : {'indicator': [{
            'title' : {"text": f"On Ventilator<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['Ventilator'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])

        fig = go.Figure()
        temp = df.loc[df['confirmed_positive_ventilator'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['confirmed_positive_ventilator'].tail(1).values[0],number = {'font': {'size': 60}},))

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive_ventilator'],marker_color='#54CAF1',visible=True, opacity=0.5))

        fig.update_layout(
            template = {'data' : {'indicator': [{
            'title' : {"text": f"On Ventilator<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['confirmed_positive_ventilator'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="on ventilator", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Cases

def total_cases_plot(region='ontario'):

    if region=='ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['Positives'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['Positives'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['Positives'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    else:
        df = vis.get_phus()
        df = df.loc[df.region == PHU[region]]
        df['value'] = df.value.cumsum()
        df['Date'] = pd.to_datetime(df['date'])

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="cases", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['value'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )




    div = fig.to_json()
    print(region)
    p = Viz.query.filter_by(header="cases",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_cases_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['New positives'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"New Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['New positives'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['New positives'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )
    else:
        df = vis.get_phus()
        df = df.loc[df.region == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="new cases", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['value'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"New Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="new cases",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def recovered_plot():
    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Resolved'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"Recovered Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Resolved'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Resolved'],marker_color='#497787', visible=True, opacity=0.5))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
        paper_bgcolor="#DFE7EA",)

    div = fig.to_json()
    p = Viz.query.filter_by(header="recovered").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_deaths_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['Deaths'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['Deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )
    else:
        df = vis.get_phudeath()
        df = df.loc[df.region == PHU[region]]
        df['value'] = df.value.cumsum()
        df['Date'] = pd.to_datetime(df['date'])
        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="deaths", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['value'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="deaths",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_deaths_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['New deaths'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"New Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['New deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['New deaths'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )
    else:
        df = vis.get_phudeath()
        df = df.loc[df.region == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="new deaths", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['value'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"New Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="new deaths", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ltc_cases_plot(region='ontario'):

    if region == 'ontario':
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=0"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Cases Total'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total LTC Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['LTC Cases Total'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Cases Total'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
    )

    else:
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=689073638"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.loc[df.PHU == PHU[region]]

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="long term care cases", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number",
            value = df.groupby('Date')['Confirmed Resident Cases'].sum().tail(1).values[0],
            number = {'font': {'size': 60}},))



        fig.update_layout(
            showlegend=False,
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Confirmed Resident Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
            legend_orientation="h",
            )

    div = fig.to_json()
    p = Viz.query.filter_by(header="long term care cases", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ltc_deaths_plot(region='ontario'):
    if region == 'ontario':
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=0"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Deaths'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Total LTC Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['LTC Deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Deaths'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
    )
    else:
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=689073638"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.loc[df.PHU == PHU[region]]

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="long term care deaths", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number",
            value = df.groupby('Date')['Resident Deaths'].sum().tail(1).values[0],
            number = {'font': {'size': 60}},))



        fig.update_layout(
            showlegend=False,
            template = {'data' : {'indicator': [{
                'title' : {"text": f"Confirmed Resident Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
            legend_orientation="h",
            )


    div = fig.to_json()
    p = Viz.query.filter_by(header="long term care deaths", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ltc_outbreaks_plot(region='ontario'):
    if region == 'ontario':
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=0"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Homes'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'title' : {"text": f"# of LTC Outbreaks<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['LTC Homes'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Homes'],marker_color='#497787', visible=True, opacity=0.5))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
    )
    else:
        url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=689073638"
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.loc[df.PHU == PHU[region]]

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="long term care outbreaks", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number",
            value = df.groupby('Date')['LTC Home'].count().tail(1).values[0],
            number = {'font': {'size': 60}},))



        fig.update_layout(
            showlegend=False,
            template = {'data' : {'indicator': [{
                'title' : {"text": f"# of LTC Outbreaks<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
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
            plot_bgcolor='#E4F7FD',
            paper_bgcolor="#E4F7FD",
            legend_orientation="h",
            )

    div = fig.to_json()
    p = Viz.query.filter_by(header="long term care outbreaks", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def rt_analysis_plot(region='Ontario'):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1086007065"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date'] = pd.to_datetime(df['date'])

    if region=='Ontario':
        df = df.loc[df.region == region]
    else:
        df = df.loc[df.region == PHU[region]]

    if len(df) <= 0:
        div = sql.null()
        p = Viz.query.filter_by(header="rt analysis", phu=region).first()
        p.html = div
        db.session.add(p)
        db.session.commit()
        return

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['ML'].tail(1).values[0],
        number = {'font': {'size': 60}},))

    fig.add_trace(go.Scatter(x=df.date,y=df.ML,marker_color='#5E5AA1',visible=True,opacity=0.5))

    fig.add_trace(go.Scatter(x=df.date,y=df.Low,
        fill=None,
        mode='lines',
        line_color='#4F4B99',opacity=0.1
        ))
    fig.add_trace(go.Scatter(x=df.date,y=df.High,
        fill='tonexty',
        mode='lines', line_color='#4F4B99',opacity=0.1))


    fig.update_layout(
        showlegend=False,
        template = {'data' : {'indicator': [{
            'title' : {"text": f"<span style='font-size:0.5em>Basic Reproduction Number (<a href='https://en.wikipedia.org/wiki/Basic_reproduction_number'>R<sub>t</sub> value</a>)</span><br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['ML'].tail(2).values[0],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': False,'visible':True},
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

    fig.add_shape(
            type="line",
            xref="paper",
            yref="y",
            x0=0,
            y0=1,
            x1=1,
            y1=1,
            line=dict(
                color="red",
                width=2,
            ),
        )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#E0DFED',
        paper_bgcolor="#E0DFED",
        legend_orientation="h",
        )

    div = fig.to_json()

    p = Viz.query.filter_by(header="rt analysis",phu=region.lower()).first()
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
    p = Viz.query.filter_by(header="transit mobility").first()
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
        fig.add_trace(go.Scatter(name=item,x=df.loc[df.category == item].date,y=df.loc[df.category == item]['value'],visible=True, opacity=0.5))


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
    p = Viz.query.filter_by(header="retail mobility").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Capacity
def icu_ontario_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_icu_capacity_province()
        df['Date'] = pd.to_datetime(df['date'])
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])


    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_beds'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )

    fig.update_layout(
        template = {'data' : {'indicator': [{
        'title' : {"text": f"ICU Beds Left<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['residual_beds'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_beds'],marker_color='#497787', visible=True, opacity=0.5))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="residual beds",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ventilator_ontario_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_icu_capacity_province()
        df['Date'] = pd.to_datetime(df['date'])
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_ventilators'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
        'title' : {"text": f"Ventilators Left<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['residual_ventilators'].iloc[-2],
                      }},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_ventilators'],marker_color='#497787', visible=True, opacity=0.5))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
)

    div = fig.to_json()
    p = Viz.query.filter_by(header="residual ventilators",phu=region).first()
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
        plot_bgcolor='#FFF',
        paper_bgcolor="#FFF",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="icu projections").first()
    p.html = div
    db.session.add(p)
    db.session.commit()
    return

def blank_plot():
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number",
    ),
                 )


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor='#FFF',
        paper_bgcolor="#FFF",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Blank Plot").first()
    p.html = div
    db.session.add(p)
    db.session.commit()


    return


## Health Care Workers

def ltc_staff_plot(region="ontario"):
    url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=0"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Staff'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"LTC Staff Infected<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Staff'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Staff'],marker_color='#497787', visible=True, opacity=0.5))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
        yaxis = {'showgrid': False,'visible':False},
        title={'text':f"",
                'y':0.95,
                'x':1,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#F0D2C9',
        paper_bgcolor="#F0D2C9",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="long term care staff cases", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def hospital_staff_plot():

    url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=0"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Hospital Staff'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'title' : {"text": f"Hospital Staff Infected<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Hospital Staff'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Hospital Staff'],marker_color='#497787', visible=True, opacity=0.5))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True},
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
        plot_bgcolor='#F0D2C9',
        paper_bgcolor="#F0D2C9",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="hospital staff cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return
