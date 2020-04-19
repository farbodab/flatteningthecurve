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
            size=22,
            color="#000"
        )
    )
    div = fig.to_html(full_html=True)

    p = Viz.query.filter_by(header="Total Cases").first()
    p.html = div
    db.session.add(p)
    db.session.commit()

    return
