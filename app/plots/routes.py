from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
import plotly.graph_objects as go
from datetime import datetime
from datetime import timedelta
import requests
from app import db
from app.models import *
from app.plots import bp
import pandas as pd
import io
from app.api import vis
from sqlalchemy import sql
import numpy as np
from app.tools.curvefit.core.model import CurveModel
from app.tools.curvefit.core.functions import gaussian_cdf, gaussian_pdf

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
 'city_of_toronto':'City of Toronto Health Unit',
 'huron_perth_county':'Huron Perth Public Health Unit'}


## Tests

def new_tests_plot():

    df = vis.get_testresults()
    df['Date'] = pd.to_datetime(df['Date'])

    fig = go.Figure()
    temp = df.loc[df['New tests'].notna()]

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['New tests'].tail(1).values[0],number = {'font': {'size': 60}},))


    fig.add_trace(go.Scatter(x=temp.Date,y=temp['New tests'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
    fig.add_trace(go.Scatter(x=df.Date,y=temp['New tests'].rolling(7).mean(),line=dict(color='red',width=3), opacity=0.5,name="7 Day Average"))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New tests'].iloc[-2],
                      'increasing': {'color':'green'},
                      'decreasing': {'color':'red'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"New Tests<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",)


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
        value = df['Total tested'].tail(1).values[0],
        number = {'font': {'size': 60}},
        ))

    # fig.add_trace(go.Scatter(x=temp.Date,y=temp['Total tested'],line=dict(color='#5E5AA1',dash='dot'), visible=True, opacity=0.5, name="Value"))
    fig.add_trace(go.Scatter(x=df.Date,y=df['Total tested'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Total tested'].iloc[-2],
                  'increasing': {'color':'green'},
                  'decreasing': {'color':'red'}}},
        ]
                         }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True,'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Total Tested<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
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
    number = {'font': {'size': 60}}
    ))

    fig.add_trace(go.Scatter(x=temp.Date,y=temp['New Positive pct'],line=dict(color='#FFF', dash='dot'),visible=True, opacity=0.5, name="Value"))
    fig.add_trace(go.Scatter(x=df.Date,y=temp['New Positive pct'].rolling(7).mean(),line=dict(color='red',width=3), opacity=0.5,name="7 Day Average"))



    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['New Positive pct'].iloc[-2]*100,
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text': f"New Positive %<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
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


    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Under Investigation'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
    fig.add_trace(go.Scatter(x=df.Date,y=temp['Under Investigation'].rolling(7).mean(),line=dict(color='red',width=3), opacity=0.5,name="7 Day Average"))

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['Under Investigation'].iloc[-2],
                      'increasing': {'color':'grey'},
                      'decreasing': {'color':'grey'}}},
            ]
                             }})

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Under Investigation<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",)

    div = fig.to_json()
    p = Viz.query.filter_by(header="under investigation").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Hospital
def in_hospital_plot(region='ontario'):
    if region=='ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])

        fig = go.Figure()
        temp = df.loc[df['Hospitalized'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['Hospitalized'].tail(1).values[0],number = {'font': {'size': 60}},))
        fig.add_trace(go.Scatter(x=temp.Date,y=temp['Hospitalized'],line=dict(color='red', width=3),visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['Hospitalized'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"COVID-19 Patients In Hospital<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",)

    div = fig.to_json()
    p = Viz.query.filter_by(header="in hospital", phu=region).first()
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
        fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU'],line=dict(color='red', width=3),visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['ICU'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"COVID-19 Patients In ICU<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",)
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])

        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="in icu", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()
        temp = df.loc[df['confirmed_positive'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['confirmed_positive'].tail(1).values[0],number = {'font': {'size': 60}},))

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive'],line=dict(color='red', width=3),visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            template = {'data' : {'indicator': [{
            'title' : {"text": f"COVID-19 Patients In ICU<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>"},
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['confirmed_positive'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':"",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
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

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['Ventilator'],line=dict(color='red', width=3),visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=temp.Date,y=temp['Ventilator'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['Ventilator'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True,'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':False},
            title={'text':f"COVID-19 Patients On Ventilator<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
    )
    else:
        df = vis.get_icu_capacity_phu()
        df = df.loc[df.PHU == PHU[region]]
        df['Date'] = pd.to_datetime(df['date'])
        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="on ventilator", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return

        fig = go.Figure()
        temp = df.loc[df['confirmed_positive_ventilator'].notna()]
        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = temp['confirmed_positive_ventilator'].tail(1).values[0],number = {'font': {'size': 60}},))

        fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive_ventilator'],line=dict(color='red', width=3),visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=temp.Date,y=temp['confirmed_positive_ventilator'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'reference': df['confirmed_positive_ventilator'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"COVID-19 Patients On Ventilator<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="on ventilator", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Cases

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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d",'reference': df['New positives'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['New positives'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
        fig.add_trace(go.Scatter(x=df.Date,y=df['New positives'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"New Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h")

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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d",'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
        fig.add_trace(go.Scatter(x=df.Date,y=df['value'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"New Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="new cases",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def active_cases_plot(region='ontario'):

    if region == 'ontario':
        df = vis.get_testresults()
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.loc[df['Active'].notna()]

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['Active'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['Active'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['Active'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['Active'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Active Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h")


    div = fig.to_json()
    p = Viz.query.filter_by(header="active cases",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['Positives'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['Positives'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['Positives'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['value'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
    )
    div = fig.to_json()
    print(region)
    p = Viz.query.filter_by(header="cases",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def recovered_plot(region='ontario'):
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
            'mode' : "number+delta+gauge",
            'delta' : {'valueformat':"d", 'reference': df['Resolved'].iloc[-2],
                  'increasing': {'color':'green'},
                  'decreasing': {'color':'red'}}},
        ]
                         }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Resolved'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
    # fig.add_trace(go.Scatter(x=df.Date,y=df['Resolved'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Recovered Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h")

    div = fig.to_json()
    p = Viz.query.filter_by(header="recovered", phu=region).first()
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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['Deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['value'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['New deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['New deaths'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
        fig.add_trace(go.Scatter(x=df.Date,y=df['New deaths'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"New Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['value'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['value'],line=dict(color='#FFF', dash='dot'), visible=True, opacity=0.5, name="Value"))
        fig.add_trace(go.Scatter(x=df.Date,y=df['value'].rolling(7).mean(),line=dict(color='red', width=3), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"New Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",

                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
            plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
        df = df.dropna(how='any')

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Cases Total'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )

        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['LTC Cases Total'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})

        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Cases Total'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Cases Total'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total LTC Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Confirmed Resident Cases<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
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
        df = df.dropna(how='any')

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Deaths'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['LTC Deaths'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Deaths'],line=dict(color='red', width=3), visible=True, opacity=0.5,name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Deaths'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Total LTC Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Confirmed Resident Deaths<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
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
        df = df.dropna(how='any')

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number+delta",
            value = df['LTC Homes'].tail(1).values[0],
            number = {'font': {'size': 60}}
        ),
                     )





        fig.update_layout(
            template = {'data' : {'indicator': [{
                'mode' : "number+delta+gauge",
                'delta' : {'valueformat':"d", 'reference': df['LTC Homes'].iloc[-2],
                          'increasing': {'color':'red'},
                          'decreasing': {'color':'green'}}},
                ]
                                 }})



        fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Homes'],line=dict(color='red', width=3), visible=True, opacity=0.5,name="Value"))
        # fig.add_trace(go.Scatter(x=df.Date,y=df['LTC Homes'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"# of LTC Homes with Outbreaks<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
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
                'mode' : "number",
            },
                ]
                                 }})


        fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"# of LTC Homes with Outbreaks<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#FFF"
            )
        )

        fig.update_layout(
            margin=dict(l=0, r=20, t=30, b=50),
           plot_bgcolor="#343332",
            paper_bgcolor="#343332",
            legend_orientation="h",
            )

    div = fig.to_json()
    p = Viz.query.filter_by(header="long term care outbreaks", phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def rt_analysis_plot(region='Ontario'):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=428679599"
    df = pd.read_csv(url)
    df['date'] = pd.to_datetime(df['date_report'])

    if region=='Ontario':
        df = df.loc[df.health_region == region]
    else:
        df = df.loc[df.health_region == PHU[region]]

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

    fig.add_trace(go.Scatter(x=df.date,y=df.ML,line=dict(color='red', width=3),visible=True,opacity=0.5))

    fig.add_trace(go.Scatter(x=df.date,y=df.Low,
        fill=None,
        mode='lines',
        line_color='grey',opacity=0.1
        ))
    fig.add_trace(go.Scatter(x=df.date,y=df.High,
        fill='tonexty',
        mode='lines', line_color='grey',opacity=0.1))


    fig.update_layout(
        showlegend=False,
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'reference': df['ML'].tail(2).values[0],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"<a href='https://en.wikipedia.org/wiki/Basic_reproduction_number'>R<sub>t</sub> value</a></span><br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#FFF"
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
                color="white",
                width=2,
            ),
        )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
        )

    div = fig.to_json()

    p = Viz.query.filter_by(header="rt analysis",phu=region.lower()).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

## Mobility

def apple_mobility_plot(region='ontario'):
    df = vis.get_mobility_transportation()

    df = df.loc[df.region=='Ontario']
    fig = go.Figure()

    df = df.loc[df.transportation_type == 'driving']
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['date'])



    fig.add_trace(go.Scatter(x=df.date,y=df['value'],line=dict(color='#FFF', dash='dot'),opacity=0.5,name="Value"))
    fig.add_trace(go.Scatter(x=df.date,y=df['value'].rolling(7).mean(),line=dict(color='red', width=3),opacity=1,name="7 Day Average"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'tickformat':'%d-%b'},
        yaxis = {'showgrid': False},
        title={'text':f"Driving Mobility<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#FFF"
        )
    )

    fig.add_shape(
            type="line",
            xref="paper",
            yref="y",
            x0=0,
            y0=100,
            x1=1,
            y1=100,
            line=dict(
                color="white",
                width=2,
            ),
        )


    fig.update_layout(
        margin=dict(l=0, r=20, t=40, b=50),
       plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="transit mobility").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def retail_mobility_plot(region='ontario'):
    df = vis.get_mobility()

    df = df.loc[df.region == 'Ontario']
    df = df.loc[df.category == "Retail & recreation"]
    df['date'] = pd.to_datetime(df['date'])
    df['day'] = df['date'].dt.day_name()
    df = df.sort_values(['date'])
    date_include = datetime.strptime("2020-02-18","%Y-%m-%d")
    df = df.loc[df['date'] > date_include]

    fig = go.Figure()

    df = df.loc[df.category == "Retail & recreation"]

    fig.add_trace(go.Scatter(x=df.date,y=df['value'],line=dict(color='red', width=3),visible=True,opacity=0.5,name="Value"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Retail and Recreation<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
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
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#343332',
        paper_bgcolor="#343332",
        legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="retail mobility",phu=region).first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def work_mobility_plot(region='ontario'):
    df = vis.get_mobility()
    df = df.loc[df.region == 'Ontario']

    df['date'] = pd.to_datetime(df['date'])
    df['day'] = df['date'].dt.day_name()

    df = df.loc[df.category == "Workplace"]
    df = df.loc[~df.day.isin(["Saturday", "Sunday"])]
    df = df.sort_values(['date'])
    date_include = datetime.strptime("2020-02-18","%Y-%m-%d")
    df = df.loc[df['date'] > date_include]

    fig = go.Figure()



    fig.add_trace(go.Scatter(x=df.date,y=df['value'],line=dict(color='red', width=3),visible=True,opacity=0.5,name="Value"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Workplaces<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
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
        margin=dict(l=0, r=10, t=30, b=50),
        plot_bgcolor='#343332',
        paper_bgcolor="#343332",
        legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="work mobility",phu=region).first()
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
        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="residual beds", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return


    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_beds'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )

    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'valueformat':"d",'reference': df['residual_beds'].iloc[-2],
                  'increasing': {'color':'green'},
                  'decreasing': {'color':'red'}}},
        ]
                         }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_beds'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
    # fig.add_trace(go.Scatter(x=df.Date,y=df['residual_beds'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"ICU Beds Left<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
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
        if len(df) <= 0:
            div = sql.null()
            p = Viz.query.filter_by(header="residual ventilators", phu=region).first()
            p.html = div
            db.session.add(p)
            db.session.commit()
            return
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['residual_ventilators'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'valueformat':"d",'reference': df['residual_ventilators'].iloc[-2],
                  'increasing': {'color':'green'},
                  'decreasing': {'color':'red'}}},
        ]
                         }})



    fig.add_trace(go.Scatter(x=df.date,y=df['residual_ventilators'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
    # fig.add_trace(go.Scatter(x=df.Date,y=df['residual_ventilators'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Ventilators Left<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
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
            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
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
                'y':0.90,
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
    df = df.dropna(how='any')

    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = df['Staff'].tail(1).values[0],
        number = {'font': {'size': 60}}
    ),
                 )





    fig.update_layout(
        template = {'data' : {'indicator': [{
            'mode' : "number+delta+gauge",
            'delta' : {'valueformat':"d",'reference': df['Staff'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Staff'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
    # fig.add_trace(go.Scatter(x=df.Date,y=df['Staff'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"LTC Staff Infected<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
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
    df = df.dropna(how='any')
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
            'mode' : "number+delta+gauge",
            'delta' : {'valueformat':"d",'reference': df['Hospital Staff'].iloc[-2],
                      'increasing': {'color':'red'},
                      'decreasing': {'color':'green'}}},
            ]
                             }})



    fig.add_trace(go.Scatter(x=df.Date,y=df['Hospital Staff'],line=dict(color='red', width=3), visible=True, opacity=0.5, name="Value"))
    # fig.add_trace(go.Scatter(x=df.Date,y=df['Hospital Staff'].rolling(7).mean(),line=dict(color='#FFF', dash='dot'), opacity=0.5,name="7 Day Average"))


    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':True, 'tickformat':'%d-%b'},
        yaxis = {'showgrid': False,'visible':True},
        title={'text':f"Hospital Staff Infected<br><span style='font-size:0.5em;color:gray'>Last Updated: {df.Date.tail(1).values[0].astype('M8[D]')}</span><br>",
                'y':0.90,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#FFF"
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        plot_bgcolor="#343332",
        paper_bgcolor="#343332",
        legend_orientation="h",
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="hospital staff cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def map():

    url = "https://docs.google.com/spreadsheets/u/0/d/1TH8vQCljcNGWzq4MXRNXGp243izKGpWq5adeEbjNBlU/export?format=csv&id=1TH8vQCljcNGWzq4MXRNXGp243izKGpWq5adeEbjNBlU&gid=0"
    s=requests.get(url).content
    loc = pd.read_csv(io.StringIO(s.decode('utf-8')))

    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=166537997"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df = df.merge(loc, left_on='region', right_on='PHU Locations')

    df['text'] = "<b>" + (df['region']).astype(str) + "</b>" + '<br>Confirmed: ' + (df['cases']).astype(str) + '<br>Deaths: ' + (df['deaths']).astype(str) + '<br># of LTC Outbreaks: ' + (df['outbreaks']).astype(str)

    fig = go.Figure()

    token ="pk.eyJ1IjoiZmFyYm9kYWIiLCJhIjoiY2s4OTRmejFjMDFsZzNmbXpzamFuOXZnMiJ9.Um1dSPlv154QcsK45jH-Dw"

    fig.add_trace(go.Scattermapbox(
            lat=df.Latitude,
            lon=df.Longitude,
            mode='markers',
            visible=True,
            name="Cases",
            text=df.text,
            hoverinfo='text',
            marker=go.scattermapbox.Marker(
                size=df.cases/10,
                sizemode = 'area',
                sizemin=4,
                color='red',
                opacity=0.5
            ),
        ))

    fig.add_trace(go.Scattermapbox(
            lat=df.Latitude,
            lon=df.Longitude,
            mode='markers',
            text=df.text,
            visible=False,
            name="Deaths",
            hoverinfo='text',
            marker=go.scattermapbox.Marker(
                size=df.deaths,
                sizemode = 'area',
                sizemin=4,
                color='yellow',
                opacity=0.5
            ),
        ))

    fig.add_trace(go.Scattermapbox(
            lat=df.Latitude,
            lon=df.Longitude,
            mode='markers',
            visible=False,
            name="Outbreaks",
            text=df.text,
            hoverinfo='text',
            marker=go.scattermapbox.Marker(
                size=df.outbreaks*5,
                sizemode ='area',
                sizemin=4,
                color='green',
                opacity=0.5
            ),
        ))



    fig.update_layout(mapbox_style="dark")
    fig.update_layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=token,
            bearing=0,
            center=dict(
                lat=47.2,
                lon=-83.3
            ),
            pitch=0,
            zoom=4
        ),
    )

    fig.update_layout(
        updatemenus=[
            dict(
                type = "buttons",
                direction="right",
                pad={"r": 10, "t": 10},
                showactive=True,
                y=0.95,
                x=0.5,
                xanchor='center',
                yanchor='top',
                buttons=list([
                    dict(label="Cases",
                         method="update",
                         args=[{"visible": [True, False, False]},
                               {"title": "Cases"}]),
                    dict(label="Deaths",
                         method="update",
                         args=[{"visible": [False, True, False]},
                               {"title": "Deaths"}]),
                    dict(label="Outbreaks",
                         method="update",
                         args=[{"visible": [False, False, True]},
                               {"title": "Outbreaks"}]),
                ]),
            )
        ])


    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})


    div = fig.to_json()
    p = Viz.query.filter_by(header="map").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return


## predictive models

def predictive_plots(day=None):
    modcollab = pd.read_sql_table('predictivemodel', db.engine).sort_values(['region', 'date'])
    modcollab['date_retrieved'] = pd.to_datetime(modcollab['date_retrieved'])
    if day:
        modcollab = modcollab[modcollab.date_retrieved == day].drop(['date_retrieved'], axis=1)
    else:
        maxdate = modcollab.loc[modcollab['date_retrieved'].idxmax()]['date_retrieved']
        modcollab = modcollab[modcollab['date_retrieved'] == maxdate]

    modcollab = modcollab[modcollab['region'] == 'base_on'].copy().reset_index(drop=True)
    modcollab['New Cases'] = modcollab['cumulative_incidence'].diff()

    fisman = pd.read_sql_table('ideamodel', db.engine)
    fisman['date_retrieved'] = pd.to_datetime(fisman['date_retrieved'])
    if day:
        fisman = fisman[fisman.date_retrieved == day].drop(['date_retrieved'], axis=1)
    else:
        maxdate = fisman.loc[fisman['date_retrieved'].idxmax()]['date_retrieved']
        fisman = fisman[fisman['date_retrieved'] == maxdate]

    fisman['date'] = pd.to_datetime(fisman['date']).dt.round('d')
    fisman = fisman[fisman['source'] == 'on'].copy().sort_values(by='date').reset_index(drop=True)
    fisman['date'] = pd.to_datetime(fisman['date']).dt.round('d')
    fisman = fisman[fisman['source'] == 'on'].copy().sort_values(by='date').reset_index(drop=True)

    berry_case = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv')
    isha_case_counts = berry_case[berry_case['province']=='Ontario'].reset_index(drop=True)
    isha_case_counts['date'] = pd.to_datetime(isha_case_counts['date_report'], dayfirst=True)

    on = pd.read_sql_table('confirmedontario', db.engine)
    on['Accurate_Episode_Date'] = pd.to_datetime(on['accurate_episode_date'])
    date_include = datetime.strptime("2020-01-01","%Y-%m-%d")
    on = on.loc[on['Accurate_Episode_Date'] > date_include]
    on = on.sort_values(by='Accurate_Episode_Date')
    counts = on.set_index('Accurate_Episode_Date')

    cases_per_day = on['Accurate_Episode_Date'].value_counts().to_frame()
    cases_per_day = cases_per_day.sort_index()
    counts = on.groupby(['Accurate_Episode_Date', 'outcome1']).size().unstack()

    n = modcollab[modcollab['New Cases'].diff() > 1].index.max()

    cases_on = on['Accurate_Episode_Date'].value_counts().rename('New Cases').to_frame()
    cases_on = cases_on.sort_index().reset_index().rename(columns={'index': 'Episode Date'})
    df = cases_on.copy()

    recent_rows_to_drop = 7

    df = df.iloc[:-recent_rows_to_drop]

    start = df.iloc[0]['Episode Date']
    end = df.iloc[-1]['Episode Date']

    df = (df.set_index('Episode Date')
            .reindex(pd.date_range(start=start,
                                   end=end,
                                   freq='d'))
            .fillna(0))

    df = (df.reindex(pd.date_range(start=start,
                               end=end + pd.offsets.DateOffset(months=1),
                               freq='d'))
        .reset_index()
        .rename(columns={'index': 'Episode Date'}))

    df['days since first case'] = list(range(1, df.shape[0] + 1))
    df['province'] = 'ON'
    df['intercept'] = 1

    model = CurveModel(
        df=df.dropna(subset=['New Cases']),
        col_t='days since first case',
        col_obs='New Cases',
        col_group='province',
        col_covs=[['intercept'], ['intercept'], ['intercept']],
        param_names=['alpha', 'beta', 'p'],
        link_fun=[lambda x: x, lambda x: x, lambda x: x],
        var_link_fun=[lambda x: x, lambda x: x, lambda x: x],
        fun=gaussian_pdf
    )

    model.fit_params([3.6e-02, 9.6e+01, 2.5e+04],
                fe_gprior=[[0, np.inf],
                           [0, np.inf],
                           [0, np.inf]])

    df['IHME fit'] = model.predict(df['days since first case'])

    model2meta = {'IDEA': [fisman.dropna(subset=['reported_cases']).iloc[-1]['date'], 'rgb(200, 0, 200)'],
              'ModCollab': [modcollab.iloc[n-1]['date'], 'orange'],
              'IHME': [df.dropna(subset=['New Cases']).iloc[-recent_rows_to_drop]['Episode Date'], '#009900']}

    model2meta = pd.DataFrame(model2meta, index=['cutoff_date', 'color']).T

    f = go.Figure()

    f.add_trace(go.Scatter(
    x=df['Episode Date'],
    y=df['New Cases'],
    name='Cases by Episode Date (ON confirmed positive)',
    mode='markers',
    marker_symbol='square',
    marker_color='#80b0ff', # '#009900',
    marker_size=8,
    legendgroup='Ontario-data'
    ))

    f.add_trace(go.Scatter(
        x=isha_case_counts['date'],
        y=isha_case_counts['cases'],
        name='Cases by Report Date (Berry et al.)',
        mode='markers',
        marker_color='#3070ff',# 'rgb(200, 0, 200)',
        showlegend=True,
        marker_size=7,
        legendgroup='Fisman-data'
    ))

    df_before = df[df['Episode Date'] <= model2meta.loc['IHME', 'cutoff_date']]
    df_after = df[df['Episode Date'] >= model2meta.loc['IHME', 'cutoff_date']]

    for data, opacity, showlegend in zip([df_before, df_after], [0.2, 1], [False, True]):
        f.add_trace(go.Scatter(
            x=data['Episode Date'],
            y=data['IHME fit'],
            name='Forecast (IHME-style)',
            mode='lines',
            marker_color='#009900',
            marker_size=5,
            showlegend=showlegend,
            opacity=opacity,
            legendgroup='Ontario-forecast'
        ))

    fisman_before = fisman[fisman['date'] <= model2meta.loc['IDEA', 'cutoff_date']]
    fisman_after = fisman[fisman['date'] >= model2meta.loc['IDEA', 'cutoff_date']]

    for data, opacity, showlegend in zip([fisman_before, fisman_after], [0.2, 1], [False, True]):
        f.add_trace(go.Scatter(
                x=data['date'],
                y=data['model_incident_cases'],
                name='Forecast (IDEA)',
                mode="lines",
                line=go.scatter.Line(color=("rgb(200, 0, 200)")),
                showlegend=showlegend,
                legendgroup='Fisman-forecast',
                opacity=opacity
        ))

    f.add_trace(go.Scatter(
        x=modcollab.loc[n:, 'date'],
        y=modcollab.loc[n:, 'New Cases'],
        name='Forecast (ModCollab - Expected Scenario)',
        mode='lines',
        marker_color='orange',
        marker_size=5,
        legendgroup='ModCollab-forecast'
    ))

    f.update_layout(template='plotly_white',
                    showlegend=True,
                    legend_x=0,
                    legend_y=1.1,
                    legend_orientation='v',
                    legend_font_size = 13, # magic underscore notation ftw
                    xaxis_title='Report date (Fisman, Berry); accurate episode date (ModCollab, ON)',
                    yaxis_title='Daily cases',
                   # hovermode="x unified",
                    font=dict(
                        family='Helvetica',
                        size=15,
                        color="#303030")
    )

    #f.show()

    div = f.to_json()
    p = Viz.query.filter_by(header="Forecasted Cases").first()
    p.viz = div
    db.session.add(p)
    db.session.commit()
    return 'success'

def to_date(datetime_str):
    datetime_object =  np.datetime64(datetime.strptime(datetime_str, '%Y-%m-%d'))
    return datetime_object

def acceleration_plot():
    today = datetime.today().strftime('%Y-%m-%d')
    canada_cases_df = pd.read_sql_table('covid', db.engine)

    days = sorted(list(set(canada_cases_df['date'].values)))

    ontario_cases_df = canada_cases_df.loc[canada_cases_df['province'].isin(['Ontario'])]
    ontario_counts = ontario_cases_df.groupby(['date']).nunique()['case_id']

    current_day = days[0]- np.timedelta64(1,'D')

    all_dates = [current_day]
    while current_day != np.datetime64(to_date("2020-12-31")):
        current_day = current_day + np.timedelta64(1,'D')
        all_dates.append(current_day)
    incident_cases = [ontario_counts.get(date,0) if date in np.arange(datetime(2020,1,24), today, timedelta(days=1)) else np.nan for date in all_dates]


    # cumulative cases
    cumulative_cases = []
    for index in range(len(incident_cases)):
        if index-1 < 0:
            cumulative_cases.append(0)
        elif np.isnan(cumulative_cases[index-1]):
            cumulative_cases.append(incident_cases[index])
        else:
            cumulative_cases.append(incident_cases[index]+cumulative_cases[index-1])

    first_deriv = []
    for index in range(len(incident_cases)):
        if index-1 < 0 or index +1 >= len(cumulative_cases) or np.isnan(cumulative_cases[index+1]):
            first_deriv.append(np.nan)
        else:
            value_next_date = all_dates[index+1]
            value_prev_date = all_dates[index-1]
            difference_date = np.timedelta64(value_next_date- value_prev_date,"D") / np.timedelta64(1,"D")

            value_next_cumulative = cumulative_cases[index+1]
            value_prev_cumulative = cumulative_cases[index-1]
            difference_cumulative = value_next_cumulative - value_prev_cumulative

            first_deriv.append(difference_cumulative/difference_date)

    # Second derivative
    second_deriv = []
    for index in range(len(incident_cases)):
        if index-1 < 0 or index +1 >= len(cumulative_cases) or np.isnan(cumulative_cases[index+1]) or np.isnan(cumulative_cases[index-1]):
            second_deriv.append(np.nan)
        else:
            value_next_date = all_dates[index+1]
            value_prev_date = all_dates[index-1]
            difference_date = np.timedelta64(value_next_date- value_prev_date,"D") / np.timedelta64(1,"D")

            value_next_firstdev = first_deriv[index+1]
            value_prev_firstdev = first_deriv[index-1]
            difference_firstdev = value_next_firstdev - value_prev_firstdev

            second_deriv.append(difference_firstdev/difference_date)

    # 7-day moving average
    moving_average = []
    std_plus = []
    std_minus = []
    for index in range(len(incident_cases)):
        start_index = index-5
        end_index = index+1

        if start_index < 0:
            moving_average.append(np.nan)
            std_plus.append(np.nan)
            std_minus.append(np.nan)
        else:
            window = second_deriv[start_index:end_index+1]
            if np.any(np.isnan(window)):
                moving_average.append(np.nan)
                std_plus.append(np.nan)
                std_minus.append(np.nan)
            else:
                mean = np.mean(window)
                stdplus = mean + np.std(window)
                stdminus = mean - np.std(window)

                moving_average.append(mean)
                std_plus.append(stdplus)
                std_minus.append(stdminus)
    final_df = pd.DataFrame({"date":all_dates, "incident_count":incident_cases, "cumulative_count":cumulative_cases, "first_derivative":first_deriv, "second_derivative":second_deriv, "moving_average":moving_average, "std_plus": std_plus, "std_minus":std_minus})
    final_df.dropna(how='any',inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=final_df.date,y=final_df.moving_average,line=dict(color='#205D86', width=3),visible=True, name="Acceleration"))

    fig.add_trace(go.Scatter(x=final_df.date,y=final_df.std_minus,
        fill=None,
        mode='lines',name="Lower bound",
        line_color='#9AADD0',opacity=0.1
        ))

    fig.add_trace(go.Scatter(x=final_df.date,y=final_df.std_plus,
        fill='tonexty', name="Upper bound",
        mode='lines', line_color='#9AADD0',opacity=0.1))

    fig.update_layout(
            xaxis =  {'showgrid': False,'visible':True},
            yaxis = {'showgrid': False,'visible':True},
            title={'text':f"Acceleration of Cases",
                    'y':0.90,
                    'x':0.5,
                   'xanchor': 'center',
                    'yanchor': 'top'},
            font=dict(
                family="Roboto",
                color="#000"
            )
        )

    fig.update_layout(
        margin=dict(l=0, r=20, t=30, b=50),
        legend_orientation="h",
        )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Acceleration of cumulative COVID-19 case counts in Ontario").first()
    p.viz = div
    db.session.add(p)
    db.session.commit()
    return 'success'
