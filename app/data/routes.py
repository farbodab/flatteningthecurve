from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime, timedelta
from datetime import date
import requests
import csv
from app import db
from app.models import *
from app.api import bp
import pandas as pd
import numpy as np
import io
import os
import urllib.request
from bs4 import BeautifulSoup
from selenium import webdriver
from app.tools.covidpdftocsv import covidpdftocsv
from app.tools.covid_19_mc_interactive_model import scrape as interactiveScraper
from app.tools.ontario_health_unit_IDEA_model import scrape as ideaScraper
import math
from sqlalchemy import text
from sqlalchemy import sql
import csv
from app.export import sheetsHelper
import re
import asyncio
import sys

@bp.route('/covid/viz', methods=['GET'])
@as_json
def new_viz():
    if os.environ['FLASK_CONFIG'] == 'development':
        url = "https://docs.google.com/spreadsheets/u/0/d/1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE/export?format=csv&id=1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE&gid=0"
    elif os.environ['FLASK_CONFIG'] == 'production':
        url = "https://docs.google.com/spreadsheets/u/0/d/1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE/export?format=csv&id=1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE&gid=803348886"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        header = row['header']
        category = row['category']
        content = row['content']
        viz = row['viz']
        thumbnail = row['thumbnail']
        text_top = row['text_top']
        text_bottom = row['text_bottom']
        mobileHeight = row['mobileHeight']
        desktopHeight = row['desktopHeight']
        page = row['page']
        order = row['order']
        row_z = row['row']
        column = row['column']
        phu = row['phu']
        tab_order = row['tab_order']
        viz_type = row['viz_type']
        viz_title = row['title']
        date = row['date']
        visible = row['visible']

        c = Viz.query.filter_by(header=header, phu=phu).first()
        if not c:
            c = Viz(header=header, category=category, content=content,
            viz=viz, thumbnail=thumbnail, mobileHeight=mobileHeight,
            text_top=text_top, text_bottom=text_bottom,
            desktopHeight=desktopHeight, page=page, order=order, row=row_z,
            column=column, phu=phu, tab_order=tab_order,viz_type=viz_type,
            viz_title=viz_title, visible=visible)
            if page == 'Analysis':
                c.date = date
            db.session.add(c)
            db.session.commit()
        else:
            c.category = category
            c.content = content
            if viz_type == 'Tableau':
                c.viz = viz
            if page == 'Analysis':
                c.date = date
            c.mobileHeight = mobileHeight
            c.desktopHeight = desktopHeight
            c.thumbnail = thumbnail
            c.text_bottom=text_bottom
            c.text_top = text_top
            c.page=page
            c.order = order
            c.row = row_z
            c.column = column
            c.tab_order = tab_order
            c.viz_type = viz_type
            c.viz_title = viz_title
            c.visible = visible
            db.session.add(c)
            db.session.commit()
    return 'success',200

@bp.route('/covid/source', methods=['GET'])
@as_json
def new_source():
    if os.environ['FLASK_CONFIG'] == 'development':
        url = "https://docs.google.com/spreadsheets/d/1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc/export?format=csv&id=1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc&gid=0"
    elif os.environ['FLASK_CONFIG'] == 'production':
        url = "https://docs.google.com/spreadsheets/d/1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc/export?format=csv&id=1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc&gid=1130875452"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        region = row['Region']
        type = row['Type']
        name = row['Name']
        source = row['Source']
        description = row['Description']
        data_feed_type = row['Data feed type']
        link = row['Link of source']
        refresh = row['Refresh']
        contributor = row['Contributor']
        contact = row['Contributor contact']
        download = row['Download Link']

        c = Source.query.filter_by(name=name).first()
        if not c:
            c = Source(region=region, type=type, name=name, source=source, description=description,
            data_feed_type=data_feed_type, link=link, refresh=refresh,
            contributor=contributor, contact=contact, download=download)
            db.session.add(c)
            db.session.commit()
        else:
            c.region = region
            c.type = type
            c.source = source
            c.description = description
            c.data_feed_type = data_feed_type
            c.link = link
            c.refresh = refresh
            c.contributor = contributor
            c.contact = contact
            c.download = download
            db.session.add(c)
            db.session.commit()
    return 'success',200

@bp.route('/covid/team', methods=['GET'])
@as_json
def new_team():
    url = "https://docs.google.com/spreadsheets/d/1Sq_KtGI1v4ABLVnJUVRGPi-hOjupSpThL9dNeeVVis0/export?format=csv&id=1Sq_KtGI1v4ABLVnJUVRGPi-hOjupSpThL9dNeeVVis0&gid=0"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        team = row['Team']
        title = row['Title']
        first_name = row['First Name']
        last_name = row['Last Name']
        education = row['Highest Ed.']
        affiliation = row['Affiliation']
        role = row['Role (Maintainers Only)']
        team_status = row['Team Status']
        linkedin =  row['LinkedIn URL']
        if first_name == first_name and first_name != '':
            c = Member.query.filter_by(first_name=first_name, last_name=last_name).first()
            if not c:
                c = Member(team=team, title=title, first_name=first_name,
                last_name=last_name, education=education, affiliation=affiliation,
                role=role, team_status=team_status,linkedin=linkedin)
                db.session.add(c)
                db.session.commit()
            else:
                c.team = team
                c.title = title
                c.education = education
                c.affiliation = affiliation
                c.role = role
                c.team_status = team_status
                c.linkedin = linkedin
                db.session.add(c)
                db.session.commit()
    return 'success',200
