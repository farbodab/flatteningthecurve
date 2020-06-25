from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime, date, timedelta
import requests
from app import db, cache
from app.models import *
from app.api import bp
from app.api.helpers import *
import pandas as pd
import io
import requests
import numpy as np
from scipy import stats as sps
from scipy.interpolate import interp1d
from dateutil import rrule

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

def get_results():
    c = Covid.query.filter_by(province="Ontario")
    df = pd.read_sql(c.statement, db.engine)
    case_count = df.groupby("date").case_id.count().cumsum().reset_index()
    case_count = case_count.loc[case_count.case_id > 100]
    df = df.loc[df.date.isin(case_count.date)]
    df = df.groupby("date").case_id.count().reset_index()
    df['case_id'] = df['case_id']*0.05
    df['case_id'] = df['case_id'].rolling(min_periods=1, window=8).sum()

    data = {'date':[], 'region':[], 'value':[]}
    data['date'] += list(range(len(df['date'].tolist())))
    data['region'] += ['Ontario']*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()

    provinces = ["Italy", "South Korea", "Singapore"]
    df = df.loc[df.date.isin(case_count.date.values)].reset_index()
    for province in provinces:
        c = Comparison.query.filter_by(province=province)
        df = pd.read_sql(c.statement, db.engine)
        case_count = df['count'].cumsum()
        df['case_count'] = case_count
        df = df.loc[df['case_count'] > 100].reset_index()
        df['count'] = df['count']*0.05
        df['count'] = df['count'].rolling(min_periods=1, window=8).sum()
        df = df.reset_index()

        data['date'] += list(range(len(df['date'].tolist())))
        data['region'] += df['province'].tolist()
        data['value'] += df['count'].tolist()

    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_phus():
    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit", "Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Southwestern":"Southwestern Public Health Unit"}
    dfs.region = dfs.region.replace(replace)
    regions = dfs.region.unique()

    data = {'date':[], 'region':[], 'value':[]}
    min = dfs['date'].min()
    max = dfs['date'].max()
    idx = pd.date_range(min, max)

    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count()
        df = df.reindex(idx, fill_value=0).reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df['index'] > date]
        df['date_str'] = df['index'].astype(str)

        data['date'] += df['index'].tolist()
        data['region'] += [region]*len(df['index'].tolist())
        data['value'] += df['case_id'].tolist()



    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df = df.sort_values('date')
    df['date_str'] = df['date'].astype(str)

    data['date'] += df['date'].tolist()
    data['region'] += ['Ontario']*len(df['date'].tolist())
    data['value'] += df['positive'].tolist()
    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_phunew():
    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    regions = dfs.region.unique()
    data = {'date':[], 'region':[], 'value':[]}
    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count().reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df.date > date]
        df['date_str'] = df['date'].astype(str)

        data['date'] += df['date'].tolist()
        data['region'] += [region]*len(df['date'].tolist())
        data['value'] += df['case_id'].tolist()

    df = dfs.groupby("date").case_id.count().reset_index()
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)

    data['date'] += df['date'].tolist()
    data['region'] += ["Ontario"]*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()
    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_growth():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'date':[], 'region':[], 'value':[]}
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df = df.groupby("date").case_id.count().cumsum().reset_index()
        df = df.loc[df.case_id > 100].reset_index()
        data['date'] += list(range(len(df['date'].tolist())))
        data['region'] += [region]*len(df['date'].tolist())
        data['value'] += df['case_id'].tolist()

    df = dfs.groupby("date").case_id.count().cumsum().reset_index()
    df = df.loc[df.case_id > 100].reset_index()

    data['date'] += list(range(len(df['date'].tolist())))
    data['region'] += ['Canada']*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        df = df['cases'].cumsum().reset_index()
        df = df.loc[df['cases'] > 100].reset_index()
        data['date'] += list(range(len(df['level_0'].tolist())))
        data['region'] += [region]*len(df['level_0'].tolist())
        data['value'] += df['cases'].tolist()

    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_growth_recent_old():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'region':[], 'value':[], 'cumulative':[]}
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df_two = df.groupby("date").case_id.count().cumsum().reset_index()
        df = df.groupby("date").case_id.count().reset_index()
        df = df.tail(7)
        data['region'] += [region]
        data['value'] += [df['case_id'].sum()]
        data['cumulative'] += [*df_two['case_id'].tail(1).values]

    df_two = dfs.groupby("date").case_id.count().cumsum().reset_index()
    df = dfs.groupby("date").case_id.count().reset_index()
    df = df.tail(7)
    data['region'] += ['Canada']
    data['value'] += [df['case_id'].sum()]
    data['cumulative'] += [*df_two['case_id'].tail(1).values]

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        df_two = df['cases'].cumsum().reset_index()
        df = df['cases'].reset_index()
        df = df.tail(7)
        data['region'] += [region]
        data['value'] += [df['cases'].sum()]
        data['cumulative'] += [*df_two['cases'].tail(1).values]

    df_final = pd.DataFrame(data, columns=['region', 'value', 'cumulative'])

    return df_final

def get_growth_recent():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'region':[], 'date':[], 'recent':[], 'cumulative':[]}

    for region in regions:
        df = dfs.loc[dfs.province == region]
        dates = df.groupby("date").case_id.count().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.case_id.cumsum().reset_index()
            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['recent'] += [recent.case_id.sum()]
            data['cumulative'] += [*cumulative.case_id.tail(1).values]


    dates = dfs.groupby("date").case_id.count().reset_index().sort_values("date").reset_index()

    # Iterate all dates
    for index, row in dates.iterrows():
        date = row['date']
        # Get subset of days before this time
        mask = (dates['date'] <= date)
        before_date = dates.loc[mask]

        cumulative = before_date.case_id.cumsum().reset_index()
        recent = before_date.tail(7)

        data['region'] += [region]
        data['date'] += [date]
        data['recent'] += [recent.case_id.sum()]
        data['cumulative'] += [*cumulative.case_id.tail(1).values]

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        dates = df.groupby("date").cases.sum().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.cases.cumsum().reset_index()
            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['recent'] += [recent.cases.sum()]
            data['cumulative'] += [*cumulative.cases.tail(1).values]

    df_final = pd.DataFrame(data, columns=['region', 'date', 'recent', 'cumulative'])

    df_final = df_final.drop(df_final.loc[df_final.cumulative<100].index)

    df_final['date_shifted'] = -999
    prev_region = 'NA'
    for index, row in df_final.iterrows():
        if row['region'] == prev_region:
            df_final.at[index,'date_shifted'] = i
            i += 1
        else:
            i = 0
            prev_region = row['region']
            df_final.at[index,'date_shifted'] = i
            i += 1

    return df_final

def get_testresults():
    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df = df.sort_values('date')
    tests ={}

    dates = []
    deaths = []
    investigations = []
    negatives = []
    positives = []
    resolveds = []
    totals = []
    new_deaths = []
    new_tests = []
    new_positives = []
    investigations_pct = []
    negatives_pct = []
    positives_pct = []
    new_positives_pct = []
    hospitalizeds = []
    icus = []
    ventilators = []

    df['new_tests'] = df.total.diff()
    df['new_deaths'] = df.deaths.diff()
    df['new_positives'] = df.positive.diff()

    for index, row in df.iterrows():
        date = str(row['date'].date())
        negative = row['negative']
        investigation = row['investigation']
        positive = row['positive']
        resolved = row['resolved']
        death = row['deaths']
        total = row['total']
        hospitalized = row['hospitalized']
        icu = row['icu']
        ventilator = row['ventilator']

        new_t = row['new_tests']
        new_d = row['new_deaths']
        new_p = row['new_positives']



        dates += [date]
        deaths += [death]
        investigations += [investigation]
        negatives += [negative]
        positives += [positive]
        resolveds += [resolved]
        totals += [total]
        hospitalizeds += [hospitalized]
        icus += [icu]
        ventilators += [ventilator]

        if row['new_tests']==row['new_tests']:
            new_tests += [new_t]
        else:
            new_tests += [0]

        if row['new_deaths']==row['new_deaths']:
            new_deaths += [new_d]
        else:
            new_deaths += [0]

        if row['new_positives']==row['new_positives']:
            new_positives += [new_p]
        else:
            new_positives += [0]


        if total:
            positives_pct += [positive/total]
            negatives_pct += [negative/total]
            investigations_pct += [investigation/total]
        else:
            positives_pct += [None]
            negatives_pct += [None]
            investigations_pct += [None]

        if new_t:
            new_positives_pct += [new_p/new_t]
        else:
            new_positives_pct += [None]

    data = {
        'Date': dates,
        'Deaths': deaths,
        'New deaths': new_deaths,
        'Under Investigation': investigations,
        'Positives': positives,
        'New positives': new_positives,
        'Negatives': negatives,
        'Total tested': totals,
        'New tests': new_tests,
        'Resolved': resolveds,
        'Positive pct': positives_pct,
        'Negative pct': negatives_pct,
        'Investigation pct': investigations_pct,
        'New Positive pct': new_positives_pct,
        'Hospitalized': hospitalizeds,
        'ICU': icus,
        'Ventilator': ventilators
    }
    df = pd.DataFrame(data, columns=['Date', 'Deaths', 'New deaths','Under Investigation', 'Positives', 'New positives','Negatives', 'Total tested', 'New tests', 'Resolved', 'Positive pct', 'Negative pct', 'Investigation pct', 'New Positive pct', 'Hospitalized','ICU', 'Ventilator'])
    df['Active'] = df['Positives'] - df['Deaths'] - df['Resolved']
    return  df

def get_icu_capacity():
    df = pd.read_sql_table('icucapacity', db.engine)

    replace = {"1. ESC":"Erie St. Clair", "2. SW": "South West", "3. WW": "Waterloo Wellington", "4. HNHB": "Hamilton Niagara Haldimand Brant", "5. CW": "Central West", "6. MH": "Mississauga Halton", "7. TC": "Toronto Central", "8. Central": "Central", "9. CE": "Central East", "10. SE": "South East", "11. Champlain": "Champlain", "12. NSM": "North Simcoe Muskoka", "13. NE": "North East", "14. NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    replace = {"L1: ESC":"Erie St. Clair", "L2: SW": "South West", "L3: WW": "Waterloo Wellington", "L4: HNHB": "Hamilton Niagara Haldimand Brant", "L5: CW": "Central West", "L6: MH": "Mississauga Halton", "L7: Toronto": "Toronto Central", "L8: Central": "Central", "L9: CE": "Central East", "L10: SE": "South East", "L11: Champlain": "Champlain", "L12: NSM": "North Simcoe Muskoka", "L13: NE": "North East", "L14: NW": "North West"}
    df.lhin = df.lhin.replace(replace)
    df = df.groupby(['date', 'lhin']).sum().reset_index()
    df = df.drop(['id'],axis=1)

    df['non_covid'] = df['critical_care_patients'] - df['confirmed_negative'] - df['confirmed_positive'] - df['suspected_covid']
    df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']
    df['vented_pct'] = df['vented_patients'] / df['vented_beds']
    df['confirmed_positive_pct'] = df['confirmed_positive'] / df['critical_care_patients']
    df['confirmed_negative_pct'] = df['confirmed_negative'] / df['critical_care_patients']
    df['suspected_covid_pct'] = df['suspected_covid'] / df['critical_care_patients']
    df['non_covid_pct'] = df['non_covid'] / df['critical_care_patients']
    df['residual_beds'] = df['critical_care_beds'] - df['critical_care_patients']
    df['residual_ventilators'] = df['vented_beds'] - df['vented_patients']

    return df

def get_icu_capacity_province():
    df = pd.read_sql_table('icucapacity', db.engine)

    df = df.groupby(['date']).sum().reset_index()
    df = df.drop(['id'],axis=1)

    df['non_covid'] = df['critical_care_patients'] - df['confirmed_negative'] - df['confirmed_positive'] - df['suspected_covid']
    df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']
    df['vented_pct'] = df['vented_patients'] / df['vented_beds']
    df['confirmed_positive_pct'] = df['confirmed_positive'] / df['critical_care_patients']
    df['confirmed_negative_pct'] = df['confirmed_negative'] / df['critical_care_patients']
    df['suspected_covid_pct'] = df['suspected_covid'] / df['critical_care_patients']
    df['non_covid_pct'] = df['non_covid'] / df['critical_care_patients']
    df['residual_beds'] = df['critical_care_beds'] - df['critical_care_patients']
    df['residual_ventilators'] = df['vented_beds'] - df['vented_patients']

    return df

def get_icu_case_status_province():
    df = pd.read_sql_table('icucapacity', db.engine)

    df = df.groupby(['date']).sum().reset_index()
    df = df.drop(['id'],axis=1)
    df = df[['date','suspected_covid','confirmed_positive']]
    data = {'date':[], 'case_status':[], 'number':[]}

    for index, row in df.iterrows():
        date = row['date']

        for item in ['suspected_covid', 'confirmed_positive']:
            data['date'] += [date]
            data['case_status'] += [item]
            data['number'] += [row[item]]

    df_final = pd.DataFrame(data, columns=['date', 'case_status', 'number'])
    return df_final

def get_mobility():
    df = pd.read_sql_table('mobilitytransportation', db.engine)
    df = df.loc[df.source == 'Google']
    df['category'] = df['transportation_type']
    return df

def get_mobility_transportation():
    df = pd.read_sql_table('mobilitytransportation', db.engine)
    return df

def get_outbreaks():
    df = pd.read_sql_table('confirmedontario', db.engine)
    df['outbreak_related'] = df['outbreak_related'].fillna('No')
    df = df.groupby(['accurate_episode_date', 'outbreak_related']).row_id.count().reset_index()
    return df


def get_tested():
    df = pd.read_sql_table('canadatesting', db.engine)
    provinces = ['Ontario', 'Quebec', 'BC', 'Alberta']
    df = df.loc[df.province.isin(provinces)]
    df.loc[df.province == 'Ontario', 'testing_adjusted'] = df['cumulative_testing'] / 13448494 * 1000
    df.loc[df.province == 'Quebec', 'testing_adjusted'] = df['cumulative_testing'] / 8164361 * 1000
    df.loc[df.province == 'BC', 'testing_adjusted'] = df['cumulative_testing'] / 4648055 * 1000
    df.loc[df.province == 'Alberta', 'testing_adjusted'] = df['cumulative_testing'] / 4067175 * 1000

    dft = pd.read_sql_table('internationaltesting', db.engine)
    regions = ['United States ', 'Italy ', 'South Korea ', 'Canada ']
    dft = dft.loc[dft.region.isin(regions)]
    dft = dft.rename(columns={"region": "province"})
    dft = dft[['date', 'province', 'cumulative_testing']]
    df = pd.concat([df,dft]).reset_index()

    df.loc[df.province == 'United States ', 'testing_adjusted'] = df['cumulative_testing'] / 330571851 * 1000
    df.loc[df.province == 'Italy ', 'testing_adjusted'] = df['cumulative_testing'] / 60480998 * 1000
    df.loc[df.province == 'South Korea ', 'testing_adjusted'] = df['cumulative_testing'] / 51259644 * 1000
    df.loc[df.province == 'Canada ', 'testing_adjusted'] = df['cumulative_testing'] / 37670879 * 1000

    return df

def get_deaths():
    data = {'date':[], 'date_shifted':[], 'province':[], 'deaths':[], 'deaths_cumulative': [], 'deaths_cumulative_recent': []}
    df = pd.read_sql_table('canadamortality', db.engine)
    df = df.groupby(['date','province']).death_id.count().reset_index()
    df = df.rename(columns={"death_id": "deaths"})
    df_tw = pd.read_sql_table('internationalmortality', db.engine)
    df_tw = df_tw.rename(columns={"country": "province"})
    df_tw = df_tw[['date', 'province', 'deaths']]
    df = pd.concat([df,df_tw])

    provinces = df.province.unique()

    for province in provinces:
        temp = df.loc[df.province == province]
        min = temp['date'].min()
        max = temp['date'].max()
        idx = pd.date_range(min, max)
        temp = temp.set_index('date')
        temp = temp.reindex(idx, fill_value=0).reset_index()
        temp['province'] = temp.province.replace(0, province)
        temp['deaths_cumulative'] = temp.deaths.cumsum()
        temp['deaths_cumulative_recent'] = temp['deaths'].rolling(min_periods=1, window=8).sum()
        temp = temp.loc[temp.deaths_cumulative > 10]
        temp = temp.reset_index()
        for index, row in temp.iterrows():
            data['date'] += [row['index']]
            data['date_shifted'] += [index]
            data['province'] += [row['province']]
            data['deaths'] += [row['deaths']]
            data['deaths_cumulative'] += [row['deaths_cumulative']]
            data['deaths_cumulative_recent'] += [row['deaths_cumulative_recent']]


    df_final = pd.DataFrame(data, columns=['date', 'date_shifted', 'province', 'deaths', 'deaths_cumulative', 'deaths_cumulative_recent'])
    return df_final

def get_cases_rolling_average():
    df_final = None

    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'region':[], 'date':[], 'average':[], 'cumulative':[]}

    for region in regions:
        df = dfs.loc[dfs.province == region]
        dates = df.groupby("date").case_id.count().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.case_id.cumsum().reset_index()
            cumulative = [*cumulative.case_id.tail(1).values]

            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['average'] += [recent.case_id.mean()]
            data['cumulative'] += cumulative

    dates = dfs.groupby("date").case_id.count().reset_index().sort_values("date").reset_index()

    # Iterate all dates
    for index, row in dates.iterrows():
        date = row['date']
        # Get subset of days before this time
        mask = (dates['date'] <= date)
        before_date = dates.loc[mask]

        cumulative = before_date.case_id.cumsum().reset_index()
        recent = before_date.tail(7)

        data['region'] += ['Canada']
        data['date'] += [date]
        data['average'] += [recent.case_id.mean()]
        data['cumulative'] += [*cumulative.case_id.tail(1).values]

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        dates = df.groupby("date").cases.sum().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.cases.cumsum().reset_index()
            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['average'] += [recent.cases.mean()]
            data['cumulative'] += [*cumulative.cases.tail(1).values]

    df_final = pd.DataFrame(data, columns=['region', 'date', 'average', 'cumulative'])

    df_final = df_final.drop(df_final.loc[df_final.cumulative<30].index)

    df_final['date_shifted'] = -999
    prev_region = 'NA'
    for index, row in df_final.iterrows():
        if row['region'] == prev_region:
            df_final.at[index,'date_shifted'] = i
            i += 1
        else:
            i = 0
            prev_region = row['region']
            df_final.at[index,'date_shifted'] = i
            i += 1

    return df_final

def get_deaths_rolling_average():
    df_final = None

    dfs = pd.read_sql_table('canadamortality', db.engine)
    regions = dfs.province.unique()
    data = {'region':[], 'date':[],  'average':[], 'cumulative':[]}

    for region in regions:
        df = dfs.loc[dfs.province == region]
        dates = df.groupby("date").death_id.count().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.death_id.cumsum().reset_index()
            cumulative = [*cumulative.death_id.tail(1).values]

            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['average'] += [recent.death_id.mean()]
            data['cumulative'] += cumulative

    dfs = pd.read_sql_table('internationalmortality', db.engine)
    regions = dfs.country.unique()

    for region in regions:
        df = dfs.loc[dfs.country == region]
        dates = df.groupby("date").deaths.sum().reset_index().sort_values("date").reset_index()

        # Iterate all dates
        for index, row in dates.iterrows():
            date = row['date']
            # Get subset of days before this time
            mask = (dates['date'] <= date)
            before_date = dates.loc[mask]

            cumulative = before_date.deaths.cumsum().reset_index()
            cumulative = [*cumulative.deaths.tail(1).values]
            recent = before_date.tail(7)

            data['region'] += [region]
            data['date'] += [date]
            data['average'] += [recent.deaths.mean()]
            data['cumulative'] += cumulative

    df_final = pd.DataFrame(data, columns=['region', 'date', 'average', 'cumulative'])
    df_final = df_final.drop(df_final.loc[df_final.cumulative<3].index)

    df_final['date_shifted'] = -999
    prev_region = 'NA'
    for index, row in df_final.iterrows():
        if row['region'] == prev_region:
            df_final.at[index,'date_shifted'] = i
            i += 1
        else:
            i = 0
            prev_region = row['region']
            df_final.at[index,'date_shifted'] = i
            i += 1

    return df_final

def get_daily_deaths():

    mortality_df = pd.read_sql_table('canadamortality', db.engine)

    regions = mortality_df.province.unique()
    data = {'date':[], 'region': [], 'daily_deaths':[]}
    for region in regions:
        daily_death_ser = mortality_df[mortality_df['province']==region].groupby('date')['death_id'].count().reset_index()
        daily_death_ser.index.name = 'date'
        daily_death_ser.name = 'daily_deaths'

        data['date'] += list(daily_death_ser['date'])
        data['region'] += [region]*len(list(daily_death_ser['date']))
        data['daily_deaths'] += list(daily_death_ser['death_id'])

    mortality_df = pd.read_sql_table('internationalmortality', db.engine)
    regions = mortality_df.country.unique()
    for region in regions:
        daily_death_ser = mortality_df[mortality_df['country']==region].groupby('date')['deaths'].sum().reset_index()
        daily_death_ser.index.name = 'date'
        daily_death_ser.name = 'daily_deaths'

        data['date'] += list(daily_death_ser['date'])
        data['region'] += [region]*len(list(daily_death_ser['date']))
        data['daily_deaths'] += list(daily_death_ser['deaths'])

    df_final = pd.DataFrame(data, columns=['date', 'region', 'daily_deaths'])

    return df_final

def get_top_causes():
    stats_df = pd.read_csv('https://raw.githubusercontent.com/alfwhitehead/coivd-datasets/master/statscan_on_causes_death.csv')
    causes_df = stats_df.loc[stats_df['Characteristics']=='Age-standardized mortality rate per 100,000 population', ['Leading causes of death (ICD-10)', 'VALUE']]
    causes_df.rename(columns={
        'Leading causes of death (ICD-10)' : 'Cause',
        'VALUE' : 'Deaths_per_100k',
    }, inplace=True)
    causes_df = causes_df[causes_df['Cause'] != 'Other causes of death']
    causes_df.sort_values(by='Deaths_per_100k', ascending=False, inplace=True)

    ONTARIO_POPULATION = 14711827

    def annualized_per_100k_to_daily(ap100k):
        return ap100k / 100000 * ONTARIO_POPULATION / 365

    causes_df['Daily_Deaths'] = causes_df['Deaths_per_100k'].apply(annualized_per_100k_to_daily)
    causes_df.head(10)

    # Make some friendly names
    def cause_to_friendly(cause):
        c2f = {
            'Malignant neoplasms [C00-C97]' : 'All Cancers',
            'Diseases of heart [I00-I09, I11, I13, I20-I51]' : 'Heart Disease',
            'Accidents (unintentional injuries) [V01-X59, Y85-Y86]' : 'Accidents / Injuries',
            'Cerebrovascular diseases [I60-I69]' : 'Stroke and Related Diseases',
            'Chronic lower respiratory diseases [J40-J47]' : 'Chronic Lower Respiratory Diseases',
            'Influenza and pneumonia [J09-J18]' : 'Influenza and Pneumonia',
            'Diabetes mellitus [E10-E14]' : 'Diabetes',
            "Alzheimer's disease [G30]" : "Alzheimer's Disease",
            'Intentional self-harm (suicide) [X60-X84, Y87.0]' : 'Suicide',
            'Chronic liver disease and cirrhosis [K70, K73-K74]' : 'Cirrhosis and Other Chronic Liver Diseases'
        }
        try:
            return c2f[cause]
        except KeyError:
            return ''

    causes_df = causes_df.head(10)
    causes_df['Cause'] = causes_df['Cause'].apply(cause_to_friendly)
    return causes_df

def get_rt_est():
    # Source Alf Whitehead Kaggle Notebook
    # https://www.kaggle.com/freealf/estimation-of-rt-from-cases
    cases_df = pd.read_sql_table('covid', db.engine)
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit", "Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Southwestern":"Southwestern Public Health Unit"}

    cases_df.region = cases_df.region.replace(replace)
    cases_df['date'] = pd.to_datetime(cases_df['date'])
    province_df = cases_df.groupby(['province', 'date'])['id'].count()
    province_df.index.rename(['region', 'date'], inplace=True)
    hr_df = cases_df.groupby(['region', 'date'])['id'].count()
    canada_df = pd.concat((province_df, hr_df))

    def prepare_cases(cases):
        # modification - Isha Berry et al.'s data already come in daily
        #new_cases = cases.diff()
        new_cases = cases

        smoothed = new_cases.rolling(7,
            win_type='gaussian',
            min_periods=1,
            # Alf: switching to right-aligned instead of centred to prevent leakage of
            # information from the future
            #center=True).mean(std=2).round()
            center=False).mean(std=2).round()

        zeros = smoothed.index[smoothed.eq(0)]
        if len(zeros) == 0:
            idx_start = 0
        else:
            last_zero = zeros.max()
            idx_start = smoothed.index.get_loc(last_zero) + 1
        smoothed = smoothed.iloc[idx_start:]
        original = new_cases.loc[smoothed.index]
        return original, smoothed

    # We create an array for every possible value of Rt
    R_T_MAX = 12
    r_t_range = np.linspace(0, R_T_MAX, R_T_MAX*100+1)

    # Gamma is 1/serial interval
    # https://wwwnc.cdc.gov/eid/article/26/6/20-0357_article
    GAMMA = 1/4

    def get_posteriors(sr, window=7, min_periods=1):
        lam = sr[:-1].values * np.exp(GAMMA * (r_t_range[:, None] - 1))

        # Note: if you want to have a Uniform prior you can use the following line instead.
        # I chose the gamma distribution because of our prior knowledge of the likely value
        # of R_t.

        # prior0 = np.full(len(r_t_range), np.log(1/len(r_t_range)))
        prior0 = np.log(sps.gamma(a=3).pdf(r_t_range) + 1e-14)

        likelihoods = pd.DataFrame(
            # Short-hand way of concatenating the prior and likelihoods
            data = np.c_[prior0, sps.poisson.logpmf(sr[1:].values, lam)],
            index = r_t_range,
            columns = sr.index)

        # Perform a rolling sum of log likelihoods. This is the equivalent
        # of multiplying the original distributions. Exponentiate to move
        # out of log.
        posteriors = likelihoods.rolling(window,
                                         axis=1,
                                         min_periods=min_periods).sum()
        posteriors = np.exp(posteriors)

        # Normalize to 1.0
        posteriors = posteriors.div(posteriors.sum(axis=0), axis=1)

        return posteriors

    def highest_density_interval(pmf, p=.95):
        # If we pass a DataFrame, just call this recursively on the columns
        if(isinstance(pmf, pd.DataFrame)):
            return pd.DataFrame([highest_density_interval(pmf[col]) for col in pmf],
                                index=pmf.columns)

        cumsum = np.cumsum(pmf.values)
        best = None
        for i, value in enumerate(cumsum):
            for j, high_value in enumerate(cumsum[i+1:]):
                if (high_value-value > p) and (not best or j<best[1]-best[0]):
                    best = (i, i+j+1)
                    break

        low = pmf.index[best[0]]
        high = pmf.index[best[1]]
        return pd.Series([low, high], index=['Low', 'High'])


    target_regions = []
    for reg, cases in canada_df.groupby(level='region'):
        if cases.max() >= 30:
            target_regions.append(reg)
    provinces_to_process = canada_df.loc[target_regions]

    results = None
    for prov_name, cases in provinces_to_process.groupby(level='region'):
        try:
            new, smoothed = prepare_cases(cases)
            try:
                posteriors = get_posteriors(smoothed)
            except Exception as e:
                print(e)
                continue
            hdis = highest_density_interval(posteriors)
            most_likely = posteriors.idxmax().rename('ML')
            result = pd.concat([most_likely, hdis], axis=1).reset_index(level=['region', 'date'])
            if results is None:
                results = result
            else:
                results = results.append(result)
        except:
            print(f'error in getting value for f{prov_name}')
    return results


def get_phudeath():
    c = CanadaMortality.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit","Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Thunder Bay": "Thunder Bay District Health Unit",
      "Southwestern":"Southwestern Public Health Unit"}
    dfs.region = dfs.region.replace(replace)
    regions = dfs.region.unique()

    data = {'date':[], 'region':[], 'value':[]}
    min = dfs['date'].min()
    max = dfs['date'].max()
    idx = pd.date_range(min, max)

    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").death_id.count()
        df = df.reindex(idx, fill_value=0).reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df['index'] > date]
        df['date_str'] = df['index'].astype(str)

        data['date'] += df['index'].tolist()
        data['region'] += [region]*len(df['index'].tolist())
        data['value'] += df['death_id'].tolist()

    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_icu_capacity_phu():
    df = pd.read_sql_table('icucapacity', db.engine)

    replace = {"1. ESC":"Erie St. Clair", "2. SW": "South West", "3. WW": "Waterloo Wellington", "4. HNHB": "Hamilton Niagara Haldimand Brant", "5. CW": "Central West", "6. MH": "Mississauga Halton", "7. TC": "Toronto Central", "8. Central": "Central", "9. CE": "Central East", "10. SE": "South East", "11. Champlain": "Champlain", "12. NSM": "North Simcoe Muskoka", "13. NE": "North East", "14. NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    replace = {"L1: ESC":"Erie St. Clair", "L2: SW": "South West", "L3: WW": "Waterloo Wellington", "L4: HNHB": "Hamilton Niagara Haldimand Brant", "L5: CW": "Central West", "L6: MH": "Mississauga Halton", "L7: Toronto": "Toronto Central", "L8: Central": "Central", "L9: CE": "Central East", "L10: SE": "South East", "L11: Champlain": "Champlain", "L12: NSM": "North Simcoe Muskoka", "L13: NE": "North East", "L14: NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    mapping = {
           "The District of Algoma Health Unit": ["North East"],
           "Brant County Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Durham Regional Health Unit": ["Central East"],
           "Grey Bruce Health Unit": ["South West"],
           "Haldimand-Norfolk Health Unit": ["Hamilton Niagara Haldimand Brant", "South West"],
           "Haliburton, Kawartha, Pine Ridge District Health Unit": ["Central East"],
           "Halton Regional Health Unit": ["Mississauga Halton", "Hamilton Niagara Haldimand Brant"],
           "City of Hamilton Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Hastings and Prince Edward Counties Health Unit": ["South East"],
           "Huron County Health Unit": ["South West"],
           "Chatham-Kent Health Unit": ["Erie St. Clair"],
           "Kingston, Frontenac, and Lennox and Addington Health Unit": ["South East"],
           "Lambton Health Unit": ["Erie St. Clair"],
           "Leeds, Grenville and Lanark District Health Unit": ["South East", "Champlain"],
           "Middlesex-London Health Unit": ["South West"],
           "Niagara Regional Area Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "North Bay Parry Sound District Health Unit": ["North East"],
           "Northwestern Health Unit": ["North West"],
           "City of Ottawa Health Unit": ["Champlain"],
           "Peel Regional Health Unit": ["Central West", "Mississauga Halton"],
           "Perth District Health Unit": ["South West"],
           "Peterborough County–City Health Unit": ["Central East"],
           "Porcupine Health Unit": ["North East"],
           "Renfrew County and District Health Unit": ["North East","Champlain"],
           "The Eastern Ontario Health Unit": ["Champlain"],
           "Simcoe Muskoka District Health Unit": ["North Simcoe Muskoka"],
           "Sudbury and District Health Unit": ["North East"],
           "Thunder Bay District Health Unit": ["North West"],
           "Timiskaming Health Unit": ["North East"],
           "Waterloo Health Unit": ["Waterloo Wellington"],
           "Wellington-Dufferin-Guelph Health Unit": ["Waterloo Wellington", "Central West"],
           "Windsor-Essex County Health Unit": ["Erie St. Clair"],
           "York Regional Health Unit": ["Central"],
           "Southwestern Public Health Unit": ["South West"],
           "City of Toronto Health Unit": ["Toronto Central", "Central East", "Central"]
           }

    df = df.groupby(['date', 'lhin']).sum().reset_index()
    df = df.drop(['id'],axis=1)

    df['non_covid'] = df['critical_care_patients'] - df['confirmed_negative'] - df['confirmed_positive'] - df['suspected_covid']
    df['residual_beds'] = df['critical_care_beds'] - df['critical_care_patients']
    df['residual_ventilators'] = df['vented_beds'] - df['vented_patients']

    data = pd.DataFrame(columns=['date','critical_care_beds','critical_care_patients','vented_beds','vented_patients','suspected_covid','suspected_covid_ventilator','confirmed_positive','confirmed_negative','confirmed_positive_ventilator','non_covid','residual_beds','residual_ventilators','PHU'])
    for item in mapping:
        temp = df.loc[df.lhin.isin(mapping[item])].groupby(['date']).sum().reset_index()
        temp['PHU'] = item
        data = data.append(temp)


    return data

def get_phu_map():
    c = CanadaMortality.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit","Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Thunder Bay": "Thunder Bay District Health Unit",
      "Southwestern":"Southwestern Public Health Unit"}
    dfs.region = dfs.region.replace(replace)

    data = {'region':[], 'cases':[],'deaths':[], 'outbreaks': [], 'critical_care_beds':[],'critical_care_patients':[],'vented_beds':[],'vented_patients':[],'suspected_covid':[],'suspected_covid_ventilator':[],'confirmed_positive':[],'confirmed_negative':[],'confirmed_positive_ventilator':[],'non_covid':[],'residual_beds':[],'residual_ventilators':[]}

    for region in PHU:
        df = dfs.loc[dfs.region == PHU[region]]
        if len(df) <= 0:
            data['region'] += [PHU[region]]
            data['deaths'] += [0]
        else:
            df = df.groupby("date").death_id.count().cumsum()
            data['region'] += [PHU[region]]
            data['deaths'] += [df.tail(1).values[0]]


    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    dfs.region = dfs.region.replace(replace)

    for region in PHU:
        df = dfs.loc[dfs.region == PHU[region]]
        if len(df) <= 0:
            data['cases'] += [0]
        else:
            df = df.groupby("date").case_id.count().cumsum()
            data['cases'] += [df.tail(1).values[0]]

    url = "https://docs.google.com/spreadsheets/d/1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U/export?format=csv&id=1pWmFfseTzrTX06Ay2zCnfdCG0VEJrMVWh-tAU9anZ9U&gid=689073638"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'])
    dfs.region = dfs.region.replace(replace)

    for region in PHU:
        temp = df.loc[df.PHU == PHU[region]]
        if len(temp) <= 0:
            data['outbreaks'] += [0]
        else:
            data['outbreaks'] += [temp.groupby('Date')['LTC Home'].count().tail(1).values[0]]


    df = pd.read_sql_table('icucapacity', db.engine)

    replace = {"1. ESC":"Erie St. Clair", "2. SW": "South West", "3. WW": "Waterloo Wellington", "4. HNHB": "Hamilton Niagara Haldimand Brant", "5. CW": "Central West", "6. MH": "Mississauga Halton", "7. TC": "Toronto Central", "8. Central": "Central", "9. CE": "Central East", "10. SE": "South East", "11. Champlain": "Champlain", "12. NSM": "North Simcoe Muskoka", "13. NE": "North East", "14. NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    replace = {"L1: ESC":"Erie St. Clair", "L2: SW": "South West", "L3: WW": "Waterloo Wellington", "L4: HNHB": "Hamilton Niagara Haldimand Brant", "L5: CW": "Central West", "L6: MH": "Mississauga Halton", "L7: Toronto": "Toronto Central", "L8: Central": "Central", "L9: CE": "Central East", "L10: SE": "South East", "L11: Champlain": "Champlain", "L12: NSM": "North Simcoe Muskoka", "L13: NE": "North East", "L14: NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    mapping = {
           "The District of Algoma Health Unit": ["North East"],
           "Brant County Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Durham Regional Health Unit": ["Central East"],
           "Grey Bruce Health Unit": ["South West"],
           "Haldimand-Norfolk Health Unit": ["Hamilton Niagara Haldimand Brant", "South West"],
           "Haliburton, Kawartha, Pine Ridge District Health Unit": ["Central East"],
           "Halton Regional Health Unit": ["Mississauga Halton", "Hamilton Niagara Haldimand Brant"],
           "City of Hamilton Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Hastings and Prince Edward Counties Health Unit": ["South East"],
           "Huron County Health Unit": ["South West"],
           "Chatham-Kent Health Unit": ["Erie St. Clair"],
           "Kingston, Frontenac, and Lennox and Addington Health Unit": ["South East"],
           "Lambton Health Unit": ["Erie St. Clair"],
           "Leeds, Grenville and Lanark District Health Unit": ["South East", "Champlain"],
           "Middlesex-London Health Unit": ["South West"],
           "Niagara Regional Area Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "North Bay Parry Sound District Health Unit": ["North East"],
           "Northwestern Health Unit": ["North West"],
           "City of Ottawa Health Unit": ["Champlain"],
           "Peel Regional Health Unit": ["Central West", "Mississauga Halton"],
           "Perth District Health Unit": ["South West"],
           "Peterborough County–City Health Unit": ["Central East"],
           "Porcupine Health Unit": ["North East"],
           "Renfrew County and District Health Unit": ["North East","Champlain"],
           "The Eastern Ontario Health Unit": ["Champlain"],
           "Simcoe Muskoka District Health Unit": ["North Simcoe Muskoka"],
           "Sudbury and District Health Unit": ["North East"],
           "Thunder Bay District Health Unit": ["North West"],
           "Timiskaming Health Unit": ["North East"],
           "Waterloo Health Unit": ["Waterloo Wellington"],
           "Wellington-Dufferin-Guelph Health Unit": ["Waterloo Wellington", "Central West"],
           "Windsor-Essex County Health Unit": ["Erie St. Clair"],
           "York Regional Health Unit": ["Central"],
           "Southwestern Public Health Unit": ["South West"],
           "City of Toronto Health Unit": ["Toronto Central", "Central East", "Central"]
           }

    df = df.groupby(['date', 'lhin']).sum().reset_index()
    df = df.drop(['id'],axis=1)

    df['non_covid'] = df['critical_care_patients'] - df['confirmed_negative'] - df['confirmed_positive'] - df['suspected_covid']
    df['residual_beds'] = df['critical_care_beds'] - df['critical_care_patients']
    df['residual_ventilators'] = df['vented_beds'] - df['vented_patients']


    data_t = pd.DataFrame(columns=['date','critical_care_beds','critical_care_patients','vented_beds','vented_patients','suspected_covid','suspected_covid_ventilator','confirmed_positive','confirmed_negative','confirmed_positive_ventilator','non_covid','residual_beds','residual_ventilators','PHU'])
    for item in mapping:
        temp = df.loc[df.lhin.isin(mapping[item])].groupby(['date']).sum().reset_index()
        temp['PHU'] = item
        data_t = data_t.append(temp)

    for region in PHU:
        temp = data_t.loc[data_t.PHU == PHU[region]]
        if len(temp) <= 0:
            data['critical_care_beds'] += [0]
            data['critical_care_patients'] += [0]
            data['vented_beds'] += [0]
            data['vented_patients'] += [0]
            data['suspected_covid'] += [0]
            data['suspected_covid_ventilator'] += [0]
            data['confirmed_positive'] += [0]
            data['confirmed_negative'] += [0]
            data['confirmed_positive_ventilator'] += [0]
            data['non_covid'] += [0]
            data['residual_beds'] += [0]
            data['residual_ventilators'] += [0]



        else:
            data['critical_care_beds'] += [temp.tail(1).critical_care_beds.values[0]]
            data['critical_care_patients'] += [temp.tail(1).critical_care_patients.values[0]]
            data['vented_beds'] += [temp.tail(1).vented_beds.values[0]]
            data['vented_patients'] += [temp.tail(1).vented_patients.values[0]]
            data['suspected_covid'] += [temp.tail(1).suspected_covid.values[0]]
            data['suspected_covid_ventilator'] += [temp.tail(1).suspected_covid_ventilator.values[0]]
            data['confirmed_positive'] += [temp.tail(1).confirmed_positive.values[0]]
            data['confirmed_negative'] += [temp.tail(1).confirmed_negative.values[0]]
            data['confirmed_positive_ventilator'] += [temp.tail(1).confirmed_positive_ventilator.values[0]]
            data['non_covid'] += [temp.tail(1).non_covid.values[0]]
            data['residual_beds'] += [temp.tail(1).residual_beds.values[0]]
            data['residual_ventilators'] += [temp.tail(1).residual_ventilators.values[0]]




    df_final = pd.DataFrame(data, columns=['region', 'cases','deaths', 'outbreaks', 'critical_care_beds','critical_care_patients','vented_beds','vented_patients','suspected_covid','suspected_covid_ventilator','confirmed_positive','confirmed_negative','confirmed_positive_ventilator','non_covid','residual_beds','residual_ventilators'])



    return df_final

def get_npi_heatmap():
    # Load the main dataset
    full_npi = pd.read_sql_table('npiinterventions', db.engine)
    # full_npi = pd.read_csv('npi_canada.csv')


    # Clean missing information and typos
    full_npi['oxford_government_response_category'] = full_npi['oxford_government_response_category'].replace({'S13 Contact Tracing': 'S13 Contact tracing',
                                                                                                               'S13 Contact-tracing': 'S13 Contact tracing'})
    full_npi['subregion'] = full_npi['subregion'].replace('NULL', '')
    full_npi['oxford_geographic_target_code'] = pd.to_numeric(full_npi['oxford_geographic_target_code'], errors='coerce').fillna(0)
    full_npi['oxford_closure_code'] = pd.to_numeric(full_npi['oxford_closure_code'], errors='coerce').fillna(0)
    full_npi['oxford_public_info_code'] = pd.to_numeric(full_npi['oxford_public_info_code'], errors='coerce').fillna(0)
    full_npi['oxford_travel_code'] = pd.to_numeric(full_npi['oxford_travel_code'], errors='coerce').fillna(0)

    # Generate temporal stringency index per province
    on = generate_cases_province('Ontario', 'Ontario', full_npi)
    qb = generate_cases_province('Quebec', 'Quebec', full_npi)
    bc = generate_cases_province('British Columbia', 'BC', full_npi)
    sk = generate_cases_province('Saskatchewan', 'Saskatchewan', full_npi)
    nb = generate_cases_province('New Brunswick', 'New Brunswick', full_npi)
    ns = generate_cases_province('Nova Scotia', 'Nova Scotia', full_npi)
    mb = generate_cases_province('Manitoba', 'Manitoba', full_npi)
    ab = generate_cases_province('Alberta', 'Alberta', full_npi)
    nv = generate_cases_province('Nunavut', 'Nunavut', full_npi)
    pei = generate_cases_province('Prince Edward Island', 'PEI', full_npi)
    nwt = generate_cases_province('Northwest Territories', 'NWT', full_npi)
    nl = generate_cases_province('Newfoundland and Labrador', 'NL', full_npi)
    yt = generate_cases_province('Yukon', 'Yukon', full_npi)

    prov_dict = {'Ontario': on,
                'Quebec': qb,
                'British Columbia': bc,
                'Saskatchewan': sk,
                'New Brunswick': nb,
                'Nova Scotia': ns,
                'Manitoba' : mb,
                'Alberta' : ab,
                'Prince Edward Island': pei,
                'Nunavut': nv,
                'Northwest Territories' : nwt,
                'Newfoundland and Labrador' : nl,
                'Yukon': yt}

    full_npi['start_date'] =  pd.to_datetime(full_npi['start_date'], infer_datetime_format='%Y-%m-%d')
    full_npi['end_date'] =  pd.to_datetime(full_npi['end_date'], infer_datetime_format='%Y-%m-%d')

    canada = pd.DataFrame({"PRENAME":[
    'Newfoundland and Labrador',
    'Prince Edward Island',
    'Nova Scotia',
    'New Brunswick',
    'Quebec',
    'Ontario',
    'Manitoba',
    'Saskatchewan',
    'Alberta',
    'British Columbia',
    'Yukon',
    'Northwest Territories',
    'Nunavut']})

    canada_overall = canada.copy(deep=True)

    final_df = {'PRENAME': [], 'Stringency': [], 'Date': []}

    today = datetime.today().strftime('%Y-%m-%d')

    # Generate heatmap for each date
    for date in np.arange(np.datetime64('2020-04-10'), np.datetime64(today)):
        canada_overall[date] = 0
        for k, v in prov_dict.items():
            idx = canada[canada['PRENAME'] == k].index[0]
            val = string_idx(v, date)
            final_df['PRENAME'] += [k]
            final_df['Stringency'] += [val]
            final_df['Date'] += [date]

    df = pd.DataFrame(final_df)

    return df

def get_source_infection():
    df = pd.read_sql_table('confirmedontario', db.engine)

    df = df.groupby(['accurate_episode_date','case_acquisitionInfo']).row_id.count().reset_index()

    return df

def get_source_infection_pct():
    df = pd.read_sql_table('confirmedontario', db.engine)
    df = df.groupby(['accurate_episode_date','case_acquisitionInfo']).row_id.count().reset_index()
    df['%'] = 100 * df['row_id'] / df.groupby('accurate_episode_date')['row_id'].transform('sum')

    return df

def get_age_trend():
    df = pd.read_sql_table('confirmedontario', db.engine)
    map = {"20s":"20-40", "30s": "20-40", "40s": "40-60", "50s": "40-60", "60s": "60-80", "70s": "60-80", "80s": "80+", "90s": "80+"}
    df['age_group'] = df['age_group'].replace(map)
    df = df.groupby(['accurate_episode_date','age_group']).row_id.count().reset_index()

    return df

def get_age_trend_outbreak():
    df = pd.read_sql_table('confirmedontario', db.engine)
    df['outbreak_related'] = df['outbreak_related'].fillna('No')
    map = {"20s":"20-40", "30s": "20-40", "40s": "40-60", "50s": "40-60", "60s": "60-80", "70s": "60-80", "80s": "80+", "90s": "80+"}
    df['age_group'] = df['age_group'].replace(map)
    df = df.groupby(['accurate_episode_date','outbreak_related','age_group']).row_id.count().reset_index()

    return df

def get_test_turn_around():
    df = pd.read_sql_table('confirmedontario', db.engine)
    df['test_turn_around'] = df['test_reported_date'] - df['specimen_reported_date']
    df['test_turn_around'] = df['test_turn_around'].dt.days
    df = df.groupby(['test_reported_date']).test_turn_around.mean().reset_index()
    return df

def get_test_turn_around_distrib():
    df = pd.read_sql_table('confirmedontario', db.engine)
    df['test_turn_around'] = df['test_reported_date'] - df['specimen_reported_date']
    df['test_turn_around'] = df['test_turn_around'].dt.days
    df = df[['test_turn_around']]
    return df

def get_weekly_new_cases():
    ont_data = pd.read_sql_table('confirmedontario', db.engine)
    start_date = datetime(2020,1,1)
    phus = {}

    for phu in set(ont_data["reporting_phu"].values):
        tmp_data = ont_data[ont_data["reporting_phu"]==phu]
        tmp_data = tmp_data.groupby(['accurate_episode_date']).count()
        tmp_data = tmp_data[['id']]
        tmp_data = tmp_data.reset_index()
        tmp_data['date_week'] = tmp_data['accurate_episode_date'].apply(lambda x: x.isocalendar()[1])
        tmp_data['date_year'] = tmp_data['accurate_episode_date'].apply(lambda x: x.isocalendar()[0])
        #ont_data

        week_count_dict = {}
        for dtime in rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=date.today()):
            dt_week = dtime.isocalendar()[1]
            dt_year = dtime.isocalendar()[0]
            d = f'{dt_year}-W{dt_week-1}'
            r = datetime.strptime(d + '-1', "%Y-W%W-%w")
            week_count_dict[r] = sum(tmp_data[(tmp_data["date_year"]==dt_year)&(tmp_data["date_week"]==dt_week)]["id"].values)

            #print(r, week_count_dict[r])
        phus[phu] = week_count_dict

    tmp_data = ont_data
    tmp_data = tmp_data.groupby(['accurate_episode_date']).count()
    tmp_data = tmp_data[['id']]
    tmp_data = tmp_data.reset_index()
    tmp_data['date_week'] = tmp_data['accurate_episode_date'].apply(lambda x: x.isocalendar()[1])
    tmp_data['date_year'] = tmp_data['accurate_episode_date'].apply(lambda x: x.isocalendar()[0])
    #ont_data

    week_count_dict = {}
    for dtime in rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=date.today()):
        dt_week = dtime.isocalendar()[1]
        dt_year = dtime.isocalendar()[0]
        d = f'{dt_year}-W{dt_week-1}'
        r = datetime.strptime(d + '-1', "%Y-W%W-%w")
        week_count_dict[r] = sum(tmp_data[(tmp_data["date_year"]==dt_year)&(tmp_data["date_week"]==dt_week)]["id"].values)

        #print(r, week_count_dict[r])
    phus["Ontario"] = week_count_dict
    phu_weekly = pd.DataFrame(phus)
    phu_weekly = phu_weekly.reset_index()
    phu_weekly = pd.melt(phu_weekly, id_vars=['index'])
    phu_weekly = phu_weekly.rename(columns={"index": "Date", "variable": "PHU", "value": "Cases"})
    return phu_weekly

def get_testing_24_hours():
    ont_data = pd.read_sql_table('confirmedontario', db.engine)
    phus = set(ont_data["reporting_phu"].values)
    #phus = ['Toronto Public Health']
    # output array
    output_arrays = {}

    # for each day in year from start date
    start_date = date(2020, 5, 1)
    end_date = date.today()
    delta = timedelta(days=1)
    while start_date <= end_date:
        #print (start_date.strftime("%Y-%m-%d"))
        for phu in phus:
            tmp = ont_data[(ont_data["reporting_phu"]==phu)&(ont_data["accurate_episode_date"]==np.datetime64(start_date))]
            counter = 0
            total = 0
            for index,row in tmp.iterrows():
                specimen_reported_date = row["specimen_reported_date"]
                test_reported_date = row["test_reported_date"]
                if specimen_reported_date == "nan" or test_reported_date == "nan":
                    difference = -1 # what do we do when one of the dates is not avaible? I treat it as not less than 24 hours.
                else:
                    difference = (test_reported_date-specimen_reported_date).days
                if difference == 0:
                    counter += 1
                total += 1
            output_arrays[phu] = output_arrays.get(phu,[]) + [[start_date,phu,counter,total]]
        tmp = ont_data[(ont_data["accurate_episode_date"]==np.datetime64(start_date))]
        counter = 0
        total = 0
        for index,row in tmp.iterrows():
            specimen_reported_date = row["specimen_reported_date"]
            test_reported_date = row["test_reported_date"]
            if specimen_reported_date == "nan" or test_reported_date == "nan":
                difference = -1 # what do we do when one of the dates is not avaible? I treat it as not less than 24 hours.
            else:
                difference = (test_reported_date-specimen_reported_date).days
            if difference == 0:
                counter += 1
            total += 1
        output_arrays["Ontario"] = output_arrays.get("Ontario",[]) + [[start_date,"Ontario",counter,total]]
        start_date += delta
    phus.add("Ontario")
    phu_dfs = []
    for phu in phus:
        df = pd.DataFrame(output_arrays[phu], columns=["Date","PHU","Number of Tests Within 24 Hours", "Number of Tests"])
        df["Percentage in 24 hours"] = df["Number of Tests Within 24 Hours"]/df["Number of Tests"]
        df['Percentage in 24 hours_7dayrolling'] = df['Percentage in 24 hours'].rolling(7).mean()
        phu_dfs.append(df)
    final_dataframe = pd.concat(phu_dfs)
    return final_dataframe

def get_rt():
    # Source Alf Whitehead Kaggle Notebook
    # https://www.kaggle.com/freealf/estimation-of-rt-from-cases
    cases_df = pd.read_sql_table('covid', db.engine)
    cases_df = cases_df.loc[cases_df.province == 'Ontario']
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit", "Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Southwestern":"Southwestern Public Health Unit"}

    cases_df.region = cases_df.region.replace(replace)
    cases_df['date'] = pd.to_datetime(cases_df['date'])
    province_df = cases_df.groupby(['province', 'date'])['id'].count()
    province_df.index.rename(['region', 'date'], inplace=True)
    hr_df = cases_df.groupby(['region', 'date'])['id'].count()
    canada_df = pd.concat((province_df, hr_df))

    def prepare_cases(cases):
        # modification - Isha Berry et al.'s data already come in daily
        #new_cases = cases.diff()
        new_cases = cases

        smoothed = new_cases.rolling(7,
            win_type='gaussian',
            min_periods=1,
            # Alf: switching to right-aligned instead of centred to prevent leakage of
            # information from the future
            #center=True).mean(std=2).round()
            center=False).mean(std=2).round()

        zeros = smoothed.index[smoothed.eq(0)]
        if len(zeros) == 0:
            idx_start = 0
        else:
            last_zero = zeros.max()
            idx_start = smoothed.index.get_loc(last_zero) + 1
        smoothed = smoothed.iloc[idx_start:]
        original = new_cases.loc[smoothed.index]
        return original, smoothed

    # We create an array for every possible value of Rt
    R_T_MAX = 12
    r_t_range = np.linspace(0, R_T_MAX, R_T_MAX*100+1)

    # Gamma is 1/serial interval
    # https://wwwnc.cdc.gov/eid/article/26/6/20-0357_article
    GAMMA = 1/4

    def get_posteriors(sr, window=7, min_periods=1):
        lam = sr[:-1].values * np.exp(GAMMA * (r_t_range[:, None] - 1))

        # Note: if you want to have a Uniform prior you can use the following line instead.
        # I chose the gamma distribution because of our prior knowledge of the likely value
        # of R_t.

        # prior0 = np.full(len(r_t_range), np.log(1/len(r_t_range)))
        prior0 = np.log(sps.gamma(a=3).pdf(r_t_range) + 1e-14)

        likelihoods = pd.DataFrame(
            # Short-hand way of concatenating the prior and likelihoods
            data = np.c_[prior0, sps.poisson.logpmf(sr[1:].values, lam)],
            index = r_t_range,
            columns = sr.index)

        # Perform a rolling sum of log likelihoods. This is the equivalent
        # of multiplying the original distributions. Exponentiate to move
        # out of log.
        posteriors = likelihoods.rolling(window,
                                         axis=1,
                                         min_periods=min_periods).sum()
        posteriors = np.exp(posteriors)

        # Normalize to 1.0
        posteriors = posteriors.div(posteriors.sum(axis=0), axis=1)

        return posteriors

    def highest_density_interval(pmf, p=.95):
        # If we pass a DataFrame, just call this recursively on the columns
        if(isinstance(pmf, pd.DataFrame)):
            return pd.DataFrame([highest_density_interval(pmf[col]) for col in pmf],
                                index=pmf.columns)

        cumsum = np.cumsum(pmf.values)
        best = None
        for i, value in enumerate(cumsum):
            for j, high_value in enumerate(cumsum[i+1:]):
                if (high_value-value > p) and (not best or j<best[1]-best[0]):
                    best = (i, i+j+1)
                    break

        low = pmf.index[best[0]]
        high = pmf.index[best[1]]
        return pd.Series([low, high], index=['Low', 'High'])


    target_regions = []
    for reg, cases in canada_df.groupby(level='region'):
        if cases.max() >= 30:
            target_regions.append(reg)
    provinces_to_process = canada_df.loc[target_regions]

    results = None
    for prov_name, cases in provinces_to_process.groupby(level='region'):
        try:
            new, smoothed = prepare_cases(cases)
            try:
                posteriors = get_posteriors(smoothed)
            except Exception as e:
                print(e)
                continue
            hdis = highest_density_interval(posteriors)
            most_likely = posteriors.idxmax().rename('ML')
            result = pd.concat([most_likely, hdis], axis=1).reset_index(level=['region', 'date'])
            if results is None:
                results = result
            else:
                results = results.append(result)
        except:
            print(f'error in getting value for f{prov_name}')

    results['PHU'] = results['region']
    return results

def get_icu_bed_occupied():

    df = pd.read_sql_table('icucapacity', db.engine)

    replace = {"1. ESC":"Erie St. Clair", "2. SW": "South West", "3. WW": "Waterloo Wellington", "4. HNHB": "Hamilton Niagara Haldimand Brant", "5. CW": "Central West", "6. MH": "Mississauga Halton", "7. TC": "Toronto Central", "8. Central": "Central", "9. CE": "Central East", "10. SE": "South East", "11. Champlain": "Champlain", "12. NSM": "North Simcoe Muskoka", "13. NE": "North East", "14. NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    replace = {"L1: ESC":"Erie St. Clair", "L2: SW": "South West", "L3: WW": "Waterloo Wellington", "L4: HNHB": "Hamilton Niagara Haldimand Brant", "L5: CW": "Central West", "L6: MH": "Mississauga Halton", "L7: Toronto": "Toronto Central", "L8: Central": "Central", "L9: CE": "Central East", "L10: SE": "South East", "L11: Champlain": "Champlain", "L12: NSM": "North Simcoe Muskoka", "L13: NE": "North East", "L14: NW": "North West"}
    df.lhin = df.lhin.replace(replace)

    mapping = {
           "The District of Algoma Health Unit": ["North East"],
           "Brant County Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Durham Regional Health Unit": ["Central East"],
           "Grey Bruce Health Unit": ["South West"],
           "Haldimand-Norfolk Health Unit": ["Hamilton Niagara Haldimand Brant", "South West"],
           "Haliburton, Kawartha, Pine Ridge District Health Unit": ["Central East"],
           "Halton Regional Health Unit": ["Mississauga Halton", "Hamilton Niagara Haldimand Brant"],
           "City of Hamilton Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "Hastings and Prince Edward Counties Health Unit": ["South East"],
           "Huron County Health Unit": ["South West"],
           "Chatham-Kent Health Unit": ["Erie St. Clair"],
           "Kingston, Frontenac, and Lennox and Addington Health Unit": ["South East"],
           "Lambton Health Unit": ["Erie St. Clair"],
           "Leeds, Grenville and Lanark District Health Unit": ["South East", "Champlain"],
           "Middlesex-London Health Unit": ["South West"],
           "Niagara Regional Area Health Unit": ["Hamilton Niagara Haldimand Brant"],
           "North Bay Parry Sound District Health Unit": ["North East"],
           "Northwestern Health Unit": ["North West"],
           "City of Ottawa Health Unit": ["Champlain"],
           "Peel Regional Health Unit": ["Central West", "Mississauga Halton"],
           "Perth District Health Unit": ["South West"],
           "Peterborough County–City Health Unit": ["Central East"],
           "Porcupine Health Unit": ["North East"],
           "Renfrew County and District Health Unit": ["North East","Champlain"],
           "The Eastern Ontario Health Unit": ["Champlain"],
           "Simcoe Muskoka District Health Unit": ["North Simcoe Muskoka"],
           "Sudbury and District Health Unit": ["North East"],
           "Thunder Bay District Health Unit": ["North West"],
           "Timiskaming Health Unit": ["North East"],
           "Waterloo Health Unit": ["Waterloo Wellington"],
           "Wellington-Dufferin-Guelph Health Unit": ["Waterloo Wellington", "Central West"],
           "Windsor-Essex County Health Unit": ["Erie St. Clair"],
           "York Regional Health Unit": ["Central"],
           "Southwestern Public Health Unit": ["South West"],
           "City of Toronto Health Unit": ["Toronto Central", "Central East", "Central"]
           }

    df = df.groupby(['date', 'lhin']).sum().reset_index()
    df = df.drop(['id'],axis=1)

    df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']

    data = pd.DataFrame(columns=['date','critical_care_pct','PHU'])
    for item in mapping:
        temp = df.loc[df.lhin.isin(mapping[item])].groupby(['date']).sum().reset_index()
        temp['PHU'] = item
        temp = temp[['date', 'critical_care_pct', 'PHU']]
        data = data.append(temp)

    df = pd.read_sql_table('icucapacity', db.engine)
    df = df.groupby(['date']).sum().reset_index()
    df = df.drop(['id'],axis=1)
    df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']
    temp = df
    temp['PHU'] = "Ontario"
    temp = temp[['date', 'critical_care_pct', 'PHU']]
    data = data.append(temp)

    return data

def get_job_data():
    df = pd.read_sql_table('weeklyjobposting', db.engine)
    df = df.loc[df.geography == 'Ontario']
    df['job_postings_count'] = df['job_postings_count'].astype(int)
    df = df.groupby(['end_date', 'group_name']).job_postings_count.mean().reset_index()

    return df

def get_duration_percentiles():
    # Author: Alf Whitehead https://www.kaggle.com/freealf/ontario-covid-19-duration-descriptive-stats

    DATE_FIELDS = ['accurate_episode_date','case_reported_date','test_reported_date','specimen_reported_date']
    metrics = ['Episode_to_Report', 'Episode_to_Specimen', 'Specimen_to_Result', 'Result_to_Report']
    percentiles = [50,80,90,95,99]
    combo_metrics = ['%s_%d' % (m, p) for m in metrics for p in percentiles]
    
    latest_sentinel = pd.read_sql_table('confirmedontario', db.engine)
    latest_date = latest_sentinel[DATE_FIELDS].max().max()

    # Correct for missing dates
    sentinel_date = pd.Timestamp.max
    for d_f in DATE_FIELDS:
        latest_sentinel[d_f] = latest_sentinel[d_f].fillna(sentinel_date)

    # Compute percentiles
    latest_sentinel['Episode_to_Report'] = (latest_sentinel['case_reported_date'] - latest_sentinel['accurate_episode_date']).dt.days
    latest_sentinel['Episode_to_Specimen'] = (latest_sentinel['specimen_reported_date'] - latest_sentinel['accurate_episode_date']).dt.days
    latest_sentinel['Specimen_to_Result'] = (latest_sentinel['test_reported_date'] - latest_sentinel['specimen_reported_date']).dt.days
    latest_sentinel['Result_to_Report'] = (latest_sentinel['case_reported_date'] - latest_sentinel['test_reported_date']).dt.days

    sentinel_delay_df = pd.DataFrame(index=pd.date_range('2020-03-01', latest_date), columns=combo_metrics)
    for crd, grp in latest_sentinel[latest_sentinel['accurate_episode_date']>=pd.to_datetime('2020-03-01')].groupby('case_reported_date'):
        for m in metrics:
            for p in percentiles:
                sentinel_delay_df.loc[crd, '%s_%d' % (m, p)] = grp[m].quantile(p/100)
    sentinel_delay_df.tail()

    def correct_sentinel(val_in_days):
        # if it's off by more than 2 years, it's due to missing data
        if (val_in_days > 730) or (val_in_days < -730):
            return np.nan
        else:
            return val_in_days

    for cm in combo_metrics:
        sentinel_delay_df[cm] = sentinel_delay_df[cm].apply(correct_sentinel)

    return sentinel_delay_df
