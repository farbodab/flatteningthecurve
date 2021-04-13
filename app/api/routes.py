from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json

from app import db, cache
from app.models import *
from app.api import bp
from app.api import vis
from app.export import restrictedsheetsHelper, sheetsHelper
from app.api.helpers import *

from datetime import datetime, timedelta
import requests
import io
import pandas as pd
import numpy as np
import glob
import os
import json
import click
import jwt
import sendgrid
from sendgrid.helpers.mail import *
import tweepy
import platform


@bp.route('/api/viz', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_viz():
    df = pd.read_sql_table('viz', db.engine)
    df = df.loc[df.page == 'Analysis']
    df = df.loc[df.visible == True]
    df = df.sort_values(by=['category', 'header'])
    data = []
    for index, row in df.iterrows():
        data.append({"header": row["header"], "category": row["category"],
        "content": row["content"], "text_top": row["text_top"], "text_bottom": row["text_bottom"],
        "viz": row["viz"], "thumbnail": row["thumbnail"],
        "mobileHeight": row["mobileHeight"],"desktopHeight": row["desktopHeight"],
        "viz_type": row["viz_type"], "date": row["date"]})
    return data


@bp.route('/api/plots', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_plots():
    df = pd.read_sql_table('viz', db.engine)
    df = df.loc[df.page != 'Analysis']
    df = df.loc[df.html.notna()]
    df = df.loc[df.order > 0]
    df = df.loc[df.visible == True]
    df = df.sort_values(by=['order'])
    data = []
    for index, row in df.iterrows():
        data.append({"header": row["header"], "order": row["order"],
        "tab": row["content"],"tab_order": row["tab_order"],
        "row": 'span '+ str(row["row"]), "column": 'span '+ str(row["column"]),
        "html": row["html"],"category": row["page"], "group": row["category"],
        "phu": row["phu"], "viz_title": row["viz_title"],"viz": row["viz"],
        "text_top": row["text_top"], "text_bottom": row["text_bottom"]})
    return data


@bp.route('/api/source', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_source():
    df = pd.read_sql_table('source', db.engine)
    df = df.sort_values(by=['name'])
    data = []
    for index, row in df.iterrows():
        data.append({"region": row["region"],"type": row["type"],"name": row["name"], "source": row["source"],
        "description": row["description"], "data_feed_type": row["data_feed_type"],
        "link": row["link"], "refresh": row["refresh"],
        "contributor": row["contributor"],"contact": row["contact"],
        "download": row["download"], "html": row["html"]})
    return data


@bp.route('/api/team', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_team():
    df = pd.read_sql_table('members', db.engine)
    df = df.sort_values(by=['team_status','last_name'])
    data = []
    for index, row in df.iterrows():
        data.append({"team": row["team"],"title": row["title"],
        "first_name": row["first_name"], "last_name": row["last_name"],
        "education": row["education"], "affiliation": row["affiliation"],
        "role": row["role"], "team_status": row["team_status"],
        "linkedin": row["linkedin"]})
    return data


@bp.route('/api/reopening', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_reopening_metrics():
    stages_df = pd.read_csv("https://docs.google.com/spreadsheets/u/0/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=0")
    weekly_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1053507889")
    testing_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1206518301")
    rt_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=428679599")
    icu_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1132863788")
    positivity_df = pd.read_csv("https://docs.google.com/spreadsheets/u/0/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1281856108")
    positivity_df = positivity_df.merge(stages_df, left_on="HR_UID", right_on="HR_UID")

    data = []
    for phu_select in PHU:
        temp_dict = {}
        temp_dict["phu"] = phu_select
        temp_dict["tracing"] = "nan"

        temp = positivity_df.loc[positivity_df.phu == phu_select]
        try:
            temp_dict["percent_positive"] = str((temp.tail(1)['% Positivity'].values[0]))
        except:
            temp_dict["percent_positive"] = "nan"

        temp = stages_df.loc[stages_df.phu == phu_select]
        try:
            temp_dict["stage"] = str((temp.tail(1)['stage'].values[0]))
        except:
            temp_dict["stage"] = "nan"


        temp = weekly_df.loc[weekly_df.PHU == PHU[phu_select]]
        temp = temp.sort_values('Date')
        try:
            temp_dict["weekly"] = str(int(temp.tail(1)['Cases'].values[0]))
        except:
            temp_dict["weekly"] = "nan"

        temp = testing_df.loc[testing_df.PHU == PHU[phu_select]]
        temp = temp.sort_values('Date')
        temp = temp.dropna(how='all')
        try:
            temp_dict["testing"] = str(int(temp.tail(1)['Percentage in 24 hours_7dayrolling'].values[0] * 100))
        except:
            temp_dict["testing"] = "nan"

        temp = rt_df.loc[rt_df.PHU == phu_select]
        temp = temp.sort_values('date')
        try:
            temp_dict["rt"] = str(temp.tail(1)['ML'].values[0])
        except:
            temp_dict["rt"] = "nan"

        temp = icu_df.loc[icu_df.phu == phu_select]
        try:
            temp_dict["icu"] = str(int(temp.tail(1)['critical_care_pct'].values[0] * 100))
        except:
            temp_dict["icu"] = "nan"




        data.append(temp_dict)

    return data


@bp.route('/api/howsmyflattening', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_my_flattening():
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615")
    HR_UID = 6
    toronto = df.loc[df.HR_UID == HR_UID]
    toronto['date'] = pd.to_datetime(toronto['date'])
    toronto['empty'] = 1 - toronto['critical_care_pct']
    toronto['non_covid'] = toronto['critical_care_pct'] - toronto['covid_pct']
    icu = toronto.dropna(how='any', subset=['critical_care_pct'])
    icu['week'] = icu['date'].dt.week
    icu['label'] = icu['date'].apply(lambda x: str(x.week) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
    icu['day-label'] = icu['date'].apply(lambda x: str(x.day) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
    icu = icu.drop_duplicates(subset=['label'],keep='last')
    icu_copy = icu.copy()
    toronto_copy = toronto.copy()
    icu = icu_copy.copy()
    icu = icu.tail(26)
    toronto = toronto_copy.copy()
    toronto = toronto.loc[toronto.date >= icu.date.min()]
    min_date = icu.date.min().month_name()
    date_max = df.date.max()
    response = {"df": toronto.to_dict(orient='records'), "icu": icu.to_dict(orient='records'), "min_date": min_date}
    return response


@bp.route('/api/summary', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_summary_metrics():
    HR_UID = request.args.get('HR_UID')
    if not HR_UID:
        HR_UID = -1
    df = get_summary(HR_UID)
    data = df.to_json(orient='records', date_format='iso')
    return data


@bp.route('/api/mail', methods=['POST'])
def subscribe():
    content = request.json
    email = content['email'].lower()
    frequency = content['frequency'].lower()
    regions = content['regions']
    date_subscribed = datetime.now()
    past = Subscribers.query.filter_by(email=email).all()
    try:
        sign_up(email,regions,frequency)
    except:
        "sign up failed"
    if past:
        for item in past:
            if item.date_subscribed:
                date_subscribed = item.date_subscribed
            db.session.delete(item)
    for region in regions:
        s = Subscribers(email=email,frequency=frequency,region=region, date_subscribed=date_subscribed)
        db.session.add(s)
    db.session.commit()

    return 'success', 200


@bp.route('/api/mail/<token>', methods=['GET'])
def unsubscribe(token):
    try:
        email = jwt.decode(token, os.getenv('SECRET_KEY'),algorithms=['HS256'])['email']
        past = Subscribers.query.filter_by(email=email).all()
        if past:
            for item in past:
                db.session.delete(item)
            db.session.commit()
            return 'You have been unsubscribed', 200
        else:
            return 'We did not find any subscricptiosn for this email address', 200
    except:
        return 'Invalid Link', 400


@bp.route('/api/auth/twitter/', methods=['GET'])
@cache.cached(timeout=600)
def get_twitter():
    print('GET')
    print(request.args)
    return 'success', 200


@bp.route('/api/vaccination', methods=['GET'])
@cache.cached(timeout=600)
def send_vaccination():
    data = get_vaccination()
    data = data.to_json(orient='records',date_format='iso')
    return data


@bp.route('/api/times', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_reopening_times():
    data = get_times()
    return data


@bp.route('/api/alerts', methods=['GET'])
@cache.cached(timeout=600)
def send_alerts():
    df = get_alerts()
    data = df.to_json(orient='records')
    return data


@bp.route('/api/epi', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_percentages():
    HR_UID = request.args.get('HR_UID')
    filter = request.args.get('filter')
    # cases = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}
    # cases_path = get_last_file(cases)
    cases_path = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=975084275"
    df = pd.read_csv(cases_path)
    date = "case_reported_date"
    df[date] = pd.to_datetime(df[date])
    df.loc[df.outbreak_related.isna(), 'outbreak_related'] = 'No'

    if HR_UID and int(HR_UID)!=0:
        df = df.loc[df.HR_UID == int(HR_UID)]

    if filter and int(filter) != -1:
        last_month = df[date].max() - pd.Timedelta(int(filter), unit='d')
        temp = df.loc[df[date] > last_month]
    else:
        temp = df
    gender = temp['client_gender'].value_counts().to_dict()
    gender_pct = temp['client_gender'].value_counts(True).to_dict()

    age_group = temp['age_group'].value_counts().to_dict()
    age_group_pct = temp['age_group'].value_counts(True).to_dict()

    ouctome = temp['outcome_1'].value_counts().to_dict()
    ouctome['Total'] = int(temp['outcome_1'].count())
    ouctome['Age'] = int(temp['outcome_1'].count())
    ouctome['Date'] = df[date].max().strftime('%B %d, %Y')
    ouctome_pct = temp['outcome_1'].value_counts(True).to_dict()
    ouctome_pct['Total'] = 1

    case_acquisition = temp['case_acquisition_info'].value_counts().to_dict()
    case_acquisition_pct = temp['case_acquisition_info'].value_counts(True).to_dict()

    outbreak = temp['outbreak_related'].value_counts().to_dict()
    outbreak_pct = temp['outbreak_related'].value_counts(True).to_dict()

    data = {
    "gender": gender,
    "age_group": age_group,
    "outcome":ouctome,
    "case_acquisition":case_acquisition,
    "outbreak":outbreak,
    "gender_pct": gender_pct,
    "age_group_pct": age_group_pct,
    "outcome_pct":ouctome_pct,
    "case_acquisition_pct":case_acquisition_pct,
    "outbreak_pct":outbreak_pct,
    }
    return json.dumps(data)


@bp.route('/api/risk', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_risk():
    location = request.args.get('location')
    if not location:
        return 'Missing location parameter', 400
    response = {}
    fsa = location[:3].upper()
    df = pd.read_csv("pccf_on.csv")
    HR_UID = df.loc[df.fsa == fsa]['HR_UID'].values
    if len(HR_UID) == 0:
        return 'Public health unit not found', 400
    df = get_summary(-1)
    risk_df = get_risk_df()
    response = {"FSA": fsa}
    risk = []
    color = []
    for phu in HR_UID:
        critical_care_patients = df.loc[df.HR_UID == phu]['critical_care_patients'].values[0]
        risk_location = risk_df.loc[risk_df.HR_UID == phu]['Status_PHU'].replace({"Lockdown":0, "Control": 1, "Restrict": 2, "Protect": 3, "Prevent": 4, "Other": 5}).values[-1]
        color.append(risk_location)
        if critical_care_patients >= 10:
            risk.append(4)
        elif critical_care_patients >= 5:
            risk.append(2)
        else:
            risk.append(0)
    response['Risk'] = sum(risk) / len(risk)
    response['Province'] = int(min(color))
    return response
