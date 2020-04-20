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

def total_cases_plot():

    df = vis.get_testresults()

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df.Date,y=df['Positives']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Cases: {df['Positives'].tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )
    div = fig.to_json()

    p = Viz.query.filter_by(header="Total Cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_tests_plot():

    df = vis.get_testresults()

    fig = go.Figure()

    temp = df.loc[df['New tests'].notna()]
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['New tests']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"New tests: {int(df['New tests'].tail(1).values[0])}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="New Tests").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def on_ventilator_plot():

    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Ventilator'].notna()]
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Ventilator']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"On Ventilator: {int(df['Ventilator'].tail(1).values[0])}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="On Ventilator").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def in_icu_plot():

    df = vis.get_testresults()

    fig = go.Figure()

    temp = df.loc[df['ICU'].notna()]
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['ICU']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"In ICU: {int(df['ICU'].tail(1).values[0])}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="In ICU").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def in_hospital_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Hospitalized'].notna()]
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Hospitalized']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"In Hospital: {int(df['Hospitalized'].tail(1).values[0])}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="In Hospital").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def recovered_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.Date,y=df['Resolved']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Recovered: {df['Resolved'].tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Recovered").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def new_cases_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.Date,y=df['New positives']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"New Cases: {df['New positives'].tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="New Cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return


def total_tests_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    temp = df.loc[df['Total tested'].notna()]
    fig.add_trace(go.Scatter(x=temp.Date,y=temp['Total tested']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Total tested: {int(df['Total tested'].tail(1).values[0])}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Tested").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def total_deaths_plot():
    df = vis.get_testresults()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.Date,y=df['Deaths']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Deaths: {df['Deaths'].tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Total Deaths").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def cases_phu_plot():
    df = vis.get_phus()

    df = df.drop(df.loc[df.region == 'Ontario'].index)

    regions = df.region.unique()

    replaced = [thing.replace("Health Unit","") for thing in regions]

    fig = go.Figure()

    df_t = df.groupby('region')['value'].sum().sort_values()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h'))

    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Cases by Public Health Unit",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Cases By PHU").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def cases_lhin_plot():
    LHIN= {
        "Erie St. Clair": 643432,
        "South West":958298,
        "Waterloo Wellington":758144,
        "Hamilton Niagara Haldimand Brant":1414803,
        "Central West":840086,
        "Mississauga Halton":1182471,
        "Toronto Central":1147368,
        "Central":1798751,
        "Central East":1607766,
        "South East":491676,
        "Champlain":1260528,
        "North Simcoe Muskoka":462026,
        "North East":566492,
        "North West":283760,
    }

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

    regions = df.LHIN.unique()

    fig = go.Figure()

    df_t = df.groupby('LHIN')['value'].sum().sort_values()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h',text=df_t.values, textposition='outside'))

    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Cases by LHIN",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Cases by LHIN").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def cases_region_plot():
    LHIN= {
        "Erie St. Clair": 643432,
        "South West":958298,
        "Waterloo Wellington":758144,
        "Hamilton Niagara Haldimand Brant":1414803,
        "Central West":840086,
        "Mississauga Halton":1182471,
        "Toronto Central":1147368,
        "Central":1798751,
        "Central East":1607766,
        "South East":491676,
        "Champlain":1260528,
        "North Simcoe Muskoka":462026,
        "North East":566492,
        "North West":283760,
    }


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

    regions = df.LHIN.unique()

    fig = go.Figure()

    pop = pd.DataFrame.from_dict(LHIN, orient='index',columns=['value']).reset_index()

    for item in Regions:
        pop.loc[pop['index'].isin(Regions[item]),'Region'] = item

    pop_dict = pop.groupby('Region')['value'].sum().to_dict()

    df_t = df.groupby('Region')['value'].sum().sort_values()
    df_t_t = pd.DataFrame(df_t).reset_index()
    df_t_t['scaled'] = 'NaN'

    for index, row in df_t_t.iterrows():
        region = row['Region']
        value = row['value']
        pop = pop_dict[region]
        df_t_t.at[index,'scaled'] = value / pop * 1000

    df_t = df.groupby('Region')['value'].sum().sort_values()

    df_t_t['scaled'] = df_t_t['scaled'].astype('float').round(2)

    fig = go.Figure()

    fig.add_trace(go.Bar(y=df_t.index, x=df_t.values, orientation='h',text=df_t.values, textposition='outside'))

    fig.add_trace(go.Bar(y=df_t_t.Region, x=df_t_t.scaled.round(2), orientation='h',text=df_t_t.scaled, textposition='outside',visible=False))

    fig.update_layout(
        xaxis =  {'showgrid': False},
        yaxis = {'showgrid': False},
        title={'text':f"Total Cases by Region",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",
            color="#000"
        ))


    fig.update_layout(
        updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=0.57,
            y=-0.1,
            buttons=list([
                dict(label="Absolute",
                     method="update",
                     args=[{"visible": [True, False]},
                           {"title": "Case Count by Region"}]),
                dict(label="Scaled",
                     method="update",
                     args=[{"visible": [False, True]},
                           {"title": "Cases Per 1000 by Region"}]),
            ]),
        )
    ])

    div = fig.to_json()
    p = Viz.query.filter_by(header="Cases by Region").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def toronto_mobility_plot():
    df = vis.get_mobility_transportation()

    df = df.loc[df.region=='Toronto']
    fig = go.Figure()

    ttype = df.transportation_type.unique()

    for thing in ttype:
        temp = df.loc[df.transportation_type == thing]
        fig.add_trace(go.Scatter(name=thing,x=temp.date,y=temp['value']))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Toronto Mobility",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
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

    fig.add_trace(go.Scatter(name='Retail & recreation',x=df.loc[df.category == 'Retail & recreation'].date,y=df.loc[df.category == 'Retail & recreation']['value']))



    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Retail & recreation {int(df.loc[df.category == 'Retail & recreation']['value'].mean())}%",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Retail Mobility").first()
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
                align="left"
            ),
            cells=dict(
                values=[df[k].tolist() for k in df[['lhin', 'residual_beds', 'residual_ventilators']].columns[:]],
                align = "left")
        )
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

    fig = go.Figure()

    for thing in ['confirmed_positive','suspected_covid','non_covid']:
        fig.add_trace(go.Bar(name=thing,y=df.lhin, x=df[thing],orientation='h'))

    fig.update_layout(barmode='stack')
    div = fig.to_json()
    p = Viz.query.filter_by(header="LHIN ICU Breakdown").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def icu_ontario_plot():
    df = vis.get_icu_capacity_province()

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df.date, y=df.residual_beds,mode="lines+markers"))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Critical Care Beds Left: {df.residual_beds.tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="ICU Ontario").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return

def ventilator_ontario_plot():
    df = vis.get_icu_capacity_province()

    fig = go.Figure()

    fig.add_trace(go.Scatter(name='Ventilators Left',x=df.date, y=df.residual_ventilators, mode="lines+markers"))

    fig.update_layout(
        xaxis =  {'showgrid': False,'visible':False},
        yaxis = {'showgrid': False},
        title={'text':f"Ventilators Left: {df.residual_ventilators.tail(1).values[0]}",
                'y':0.9,
                'x':0.5,
               'xanchor': 'center',
                'yanchor': 'top'},
        font=dict(
            family="Roboto",

            color="#000"
        )
    )

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
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor="White",
        legend_orientation="h"
    )

    div = fig.to_json()
    p = Viz.query.filter_by(header="Ventilator Ontario").first()
    p.html = div
    db.session.add(p)
    db.session.commit()
    return
