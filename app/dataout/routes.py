from flask import Flask, request, jsonify, g, render_template, make_response
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime
import requests
from app import db
from app.models import *
from app.api import bp
import pandas as pd
import io
import requests
from flask import Response
from io import StringIO

@bp.route('/data/covidtests', methods=['GET'])
def sendcovidtests():
    df = pd.read_sql_table('covidtests', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=test_data_on.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp


@bp.route('/data/covid', methods=['GET'])
def sendcovid():
    df = pd.read_sql_table('covid', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=test_data_canada.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/npi', methods=['GET'])
def sendnpi():
    df = pd.read_sql_table('npiinterventions', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=npi_canada.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/canadamortality', methods=['GET'])
def sendcanadamortality():
    df = pd.read_sql_table('canadamortality', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=canada_mortality.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/canadarecovered', methods=['GET'])
def sendcanadarecovered():
    df = pd.read_sql_table('canadarecovered', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=canada_recovered.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/internationaldata', methods=['GET'])
def sendinternationaldata():
    df = pd.read_sql_table('internationaldata', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=test_data_intl.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/internationalmortality', methods=['GET'])
def sendinternationalmortality():
    df = pd.read_sql_table('internationalmortality', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=international_mortality.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/internationalrecovered', methods=['GET'])
def sendinternationalrecovered():
    df = pd.read_sql_table('internationalrecovered', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=international_recovered.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp


@bp.route('/data/icucapacity', methods=['GET'])
def icucapacity():
    df = pd.read_sql_table('icucapacity', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=icucapacity.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/phucapacity', methods=['GET'])
def phucapacity():
    df = pd.read_sql_table('phucapacity', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=icu_capacity_on.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@bp.route('/data/source', methods=['GET'])
def source():
    df = pd.read_sql_table('source', db.engine)
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=source.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp
