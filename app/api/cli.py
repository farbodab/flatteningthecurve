from app.api import bp
from app.api.helpers import *
from app.api import vis
from app.models import *
from app import db, cache
from firebase_admin import credentials, initialize_app, storage
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import click
import pandas as pd
import numpy as np
import os
import jwt
import sendgrid
from sendgrid.helpers.mail import *
from flask import Flask, request, jsonify, g, render_template
import tweepy
import requests

@bp.cli.command('get_images')
def get_images():
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615")
    credintionals = {
    "type": "service_account",
    "project_id": "covid-data-analytics-hub",
    "private_key_id": os.getenv('email_private_key_id'),
    "private_key": os.getenv('email_private_key').replace('\\n', '\n'),
    "client_email": "firebase-adminsdk-irx4s@covid-data-analytics-hub.iam.gserviceaccount.com",
    "client_id": "108720830737287977754",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-irx4s%40covid-data-analytics-hub.iam.gserviceaccount.com"
    }
    cred = credentials.Certificate(credintionals)
    initialize_app(cred, {'storageBucket': 'covid-data-analytics-hub.appspot.com'})
    bucket = storage.bucket()
    for HR_UID in df.HR_UID.unique():
        toronto = df.loc[df.HR_UID == HR_UID]
        toronto['date'] = pd.to_datetime(toronto['date'])
        min_date = toronto.date.max() - pd.Timedelta(7*26, unit="d")
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

        toronto = toronto.loc[toronto.date >= min_date]

        min_date = toronto.date.min().month_name()
        date_max = df.date.max()

        fig = make_subplots(rows=2, cols=1, shared_xaxes=False)

        fig.add_trace(go.Scatter(x=toronto['date'], y=toronto['rolling_pop'],
                            mode='lines',
                            name='Case Incidence', legendgroup="group1",
                                line=dict(color='#1AA8D0', width=2)), row=1, col=1)


        fig.add_trace(go.Bar(name='Covid Patients', x=icu['day-label'], y=icu['covid_pct'],marker_color='#D94718', marker_line_color='#D94718',legendgroup="group"),row=2, col=1)
        fig.add_trace(go.Bar(name='Other Patients', x=icu['day-label'], y=icu['non_covid'],marker_color='#F2BB2B', marker_line_color='#F2BB2B',legendgroup="group"),row=2, col=1)
        fig.add_trace(go.Bar(name='Available Room', x=icu['day-label'], y=icu['empty'],marker_color='#1AA8D0', marker_line_color='#1AA8D0',legendgroup="group"),row=2, col=1)

        # Change the bar mode
        fig.update_layout(
            barmode='stack',
            title=f" Daily Cases + ICU Capacity Update (Since {min_date})",
            xaxis_title="Date",
            xaxis2_title="Week Ending",
            yaxis_title="Daily Cases Per 100k",
            yaxis2_title="% of ICU beds",
            yaxis2 = dict(
            tickformat= ',.0%',
            range= [0,1]
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
            yanchor="top",
            y=0.6,
            xanchor="right",
            x=1.3
            )
        )
        # print(os.getcwd())
        file = f"{HR_UID}_{date_max}.jpeg"
        path = f"{os.getcwd()}/app/static/email/{file}"
        fig.write_image(path, width=1200,height=675)
        if os.getenv('FLASK_CONFIG') == 'production':
            blob = bucket.blob(file)
            blob.upload_from_filename(path)
            blob.make_public()
            print("your file url", blob.public_url)
        else:
            print('done')

@bp.cli.command("email")
@click.argument("frequency")
def email(frequency):
    df = get_summary(-1)
    alerts = get_alerts().to_dict(orient='records')
    if len(alerts) > 0:
        alerts = alerts[0]
    vaccine = get_vaccination().to_dict(orient='records')[0]
    df['rolling_test_twenty_four'] = df['rolling_test_twenty_four'] * 100
    df['critical_care_pct'] = df['critical_care_pct'] * 100
    df['covid_pct'] = df['covid_pct'] * 100
    df = df.round(2)
    changed = df.loc[df['count'] == 1]
    date = get_times()
    past = Subscribers.query.filter_by(frequency=frequency)
    subscribers = pd.read_sql_query(past.statement, db.engine)
    emails = subscribers.email.unique()
    ontario = vis.get_testresults()
    ontario['Date'] = pd.to_datetime(ontario['Date'])
    ontario['Date'] = ontario['Date'].dt.strftime('%B %d')
    ontario = ontario.tail(1).to_dict(orient='records')[0]
    if frequency == 'daily' or frequency == 'weekly':
        for email in emails:
            temp = subscribers.loc[subscribers.email == email]
            token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
            my_regions = temp.region.unique()[:]
            temp_df = df.loc[df.HR_UID.isin(my_regions)]
            max_date = df.date.max().strftime("%Y-%m-%d")
            temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
            regions = temp_df.to_dict(orient='records')
            regions_changed = temp_changed.to_dict(orient='records')
            key = os.environ.get('EMAIL_API')
            sg = sendgrid.SendGridAPIClient(api_key=key)
            from_email = "mycovidreport@howsmyflattening.ca"
            to_email = email
            subject = "Your Personalized COVID-19 Report"
            html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
            text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
            message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject='Your personalized COVID-19 report',
            plain_text_content=text,
            html_content=html)
            if os.getenv('FLASK_CONFIG') == 'production':
                try:
                    response = sg.send(message)
                except Exception as e:
                    print("error " + e.message)
            else:
                print('done')
    elif frequency == 'change':
        for email in emails:
            temp = subscribers.loc[subscribers.email == email]
            token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
            my_regions = temp.region.unique()[:]
            temp_df = df.loc[(df.HR_UID.isin(my_regions)) & (df['count'] == 1)]
            max_date = df.date.max().strftime("%Y-%m-%d")
            if len(temp_df) > 1:
                temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
                regions = temp_df.to_dict(orient='records')
                regions_changed = temp_changed.to_dict(orient='records')
                key = os.environ.get('EMAIL_API')
                sg = sendgrid.SendGridAPIClient(api_key=key)
                from_email = "mycovidreport@howsmyflattening.ca"
                to_email = email
                subject = "Your Personalized COVID-19 Report"
                html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                message = Mail(
                from_email=from_email,
                to_emails=to_email,
                subject='Your personalized COVID-19 report',
                plain_text_content=text,
                html_content=html)
                if os.getenv('FLASK_CONFIG') == 'production':
                    try:
                        response = sg.send(message)
                        print(response.status_code)
                    except Exception as e:
                        print("error " + e.message)
                else:
                    print('done')

@bp.cli.command("tweet")
def tweet():
    mapping = {
    3526:"AlgomaHealth",
    3527:"BrantHealthUnit",
    3540:"CKPublicHealth",
    3530:"DurhamHealth",
    3558:"EOHU_tweet",
    3533:"GBPublicHealth",
    3534:"HNHealthUnit",
    3535:"HKPRDHU",
    3536:"RegionofHalton",
    3537:"hamphslib",
    3538:"HPEPublicHealth",
    3539:"HPPublicHealth",
    3541:"KFLAPH",
    3542:"lambton_ph",
    3543:"LGLHealthUnit",
    3544:"MLHealthUnit",
    3546:"NRPublicHealth",
    3547:"NBPSDHealthUnit",
    3549:"TheNWHU",
    3551:"OttawaHealth",
    3553:"regionofpeel",
    3555:"Ptbohealth",
    3556:"PorcupineHU",
    3565:"ROWPublicHealth",
    3557:"RCDHealthUnit",
    3560:"SMDhealthunit",
    3575:"SW_PublicHealth",
    3561:"PublicHealthSD",
    3562:"TBDHealthUnit",
    3563:"TimiskamingHU",
    3595:"TOPublicHealth",
    3566:"WDGPublicHealth",
    3568:"TheWECHU",
    3570:"YorkRegionGovt",
    6: "Ontario"
    }
    df = get_summary(-1)
    df['date'] = pd.to_datetime(df['date'])
    df['rolling_pop_prev'] = df['rolling_pop'].shift(7)
    df['change'] = df['rolling_pop'] - df['rolling_pop_prev']
    max_date = df.date.max().strftime("%Y-%m-%d")
    df['critical_care_pct'] = df['critical_care_pct'] * 100
    df['rolling_test_twenty_four'] = df['rolling_test_twenty_four'] * 100
    ont = df.loc[(df.date == df.date.max()) & (df.phu == 'Ontario')].tail(1).to_dict(orient='records')[0]
    df = df.loc[(df.date == df.date.max()) & (df.phu != 'Ontario')]
    risks = df.groupby('risk').count()['phu']
    df = df.sort_values(by='change')
    vaccine = get_vaccination().to_dict(orient='records')[0]
    ontario = pd.read_sql_table('ontario_covid_summary',con=db.engine)
    ontario['percent_positive'] = ontario['percent_positive_tests_in_last_day']
    ontario['percent_positive_trend'] = ontario['percent_positive'].diff(7)
    ontario['reported_date'] = pd.to_datetime(ontario['reported_date'])
    ontario['reported_date'] = ontario['reported_date'].dt.strftime('%B %d')
    ontario = ontario.tail(1).to_dict(orient='records')[0]
    date = get_times()
    up = '\U00002B06'
    down = '\U00002B07'
    text = f'\U00002615{ontario["reported_date"]}:#COVID19 in #Ontario\n{int(ontario["total_cases_change"])} new cases, {int(ontario["deaths_change"])} new deaths.\n{int(ontario["number_of_patients_hospitalized_with_covid-19"])} in hospital, {int(ontario["number_of_patients_in_icu_due_to_covid-19"])} in the ICU.\n{vaccine["previous_day_doses_administered"]} doses vaccinated yesterday ({round(vaccine["percentage_completed"],2)}% of Ontario)'
    reply1_text= f'\U0001F3E5Long Term Care Update:\nResidents: {int(ontario["total_positive_ltc_resident_cases_change"])} new cases and {int(ontario["total_ltc_resident_deaths_change"])} new deaths.\nHealth Care Workers: {int(ontario["total_positive_ltc_hcw_cases_change"])} new cases and {int(ontario["total_ltc_hcw_deaths_change"])} new deaths.'
    reply2_text = f'\U0001F9A0Variant Update:\nB117 (identified in the UK): {int(ontario["total_lineage_b.1.1.7_change"])} new cases, {int(ontario["total_lineage_b.1.1.7"])} cases todate.\nB1351 (identified in South Africa): {int(ontario["total_lineage_b.1.351_change"])} new cases, {int(ontario["total_lineage_b.1.351"])} cases todate.\nP1 (identified in Brazil): {int(ontario["total_lineage_p.1_change"])} new cases, {int(ontario["total_lineage_p.1"])} cases todate.'
    reply3_text = f"\U0001F4C8Ontario Key Indicators + Trend:\nCase Incidence: {round(ont['rolling_pop'],1)} {up if ont['rolling_pop_trend'] > 0 else down} ({date['rolling_pop'][0]})\nRt: {round(ont['rt_ml'],2)} {up if ont['rt_ml_trend'] > 0 else down} ({date['rt_ml'][0]})\nTesting < 24h: {int(ont['rolling_test_twenty_four'])}% {up if ont['rolling_test_twenty_four_trend'] > 0 else down} ({date['rolling_test_twenty_four'][0]})\nPercent Positivity: {round(ontario['percent_positive'],1)}% {up if ontario['percent_positive_trend'] > 0 else down} ({ontario['reported_date']})\nICU Occupancy: {int(ont['critical_care_pct'])}% {up if ont['critical_care_pct_trend'] > 0 else down} ({date['critical_care_pct'][0]})"
    reply4_text = f"\U0001F6A6Ontario community risk levels:\n1. High (≥ 10  new cases per 100k): {risks['High']} regions\n2. Medium (5-10 new cases per 100k): {risks['Medium']} regions\n3. Low (1-5 new cases per 100k): {risks['Low']} regions\n4. Very Low (<1 new cases per 100k): {risks['Very Low']} regions"
    reply5_text = f'\U00002B50Top 3 Curve Flatteners in the last 7 days (using daily cases per 100k):\n1.{df.phu.values[0]} (from {round(df.rolling_pop_prev.values[0],1)} to {round(df.rolling_pop.values[0],1)})\n2.{df.phu.values[1]} (from {round(df.rolling_pop_prev.values[1],1)} to {round(df.rolling_pop.values[1],1)})\n3.{df.phu.values[2]} (from {round(df.rolling_pop_prev.values[2],1)} to {round(df.rolling_pop.values[2],1)})'
    reply6_text = "\U0001F4E7Learn how the pandemic is affecting regions across the province. Sign up for a customized daily report delivered to your inbox at: https://howsmyflattening.ca/#/home"
    api_key = os.getenv('twitter_api_key')
    api_secret_key = os.getenv('twitter_api_secret_key')
    key = os.getenv('twitter_key')
    secret = os.getenv('twitter_secret')
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(key, secret)
    api = tweepy.API(auth)
    file = f"6_{max_date}.jpeg"
    path = f"{os.getcwd()}/app/static/email/{file}"
    url = f"https://storage.googleapis.com/covid-data-analytics-hub.appspot.com/{file}"
    with open(path, 'wb') as handle:
        response = requests.get(url)
        handle.write(response.content)
    if os.getenv('FLASK_CONFIG') == 'production':
        original_tweet = api.update_with_media(filename=path,status=text)
        reply1_tweet = api.update_status(status=reply1_text,
                                     in_reply_to_status_id=original_tweet.id,
                                     auto_populate_reply_metadata=True)
        reply2_tweet = api.update_status(status=reply2_text,
                                         in_reply_to_status_id=reply1_tweet.id,
                                         auto_populate_reply_metadata=True)
        reply3_tweet = api.update_status(status=reply3_text,
                                         in_reply_to_status_id=reply2_tweet.id,
                                         auto_populate_reply_metadata=True)
        reply4_tweet = api.update_status(status=reply4_text,
                                         in_reply_to_status_id=reply3_tweet.id,
                                         auto_populate_reply_metadata=True)
        reply5_tweet = api.update_status(status=reply5_text,
                                         in_reply_to_status_id=reply4_tweet.id,
                                         auto_populate_reply_metadata=True)
        reply6_tweet = api.update_status(status=reply6_text,
                                         in_reply_to_status_id=reply5_tweet.id,
                                         auto_populate_reply_metadata=True)
    else:
        print('done')
