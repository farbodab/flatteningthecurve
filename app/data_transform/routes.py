from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app import db
from app.models import *
from app.data_transform import bp
from datetime import datetime, timedelta
from datetime import date as datte
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob, os
import numpy as np
from scipy import stats as sps
from scipy.interpolate import interp1d
from sqlalchemy import sql
import subprocess
from dateutil import rrule

def convert_date(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y/%m/%d')

def get_file_path(data, step='processed', today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + step + '/'
    if data['type'] != '':
        file_name = data['table_name'] + '_' + today + '.' + data['type']
    else:
        file_name = data['table_name'] + '_' + today
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

def transform(data_in, data_out):
    load_file, load_dir = get_file_path(data_in)
    files = glob.glob(load_dir + "/*." + data_in['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.' + data_in['type'])[0]
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data_in['source_name']}/{data_in['table_name']}")
                print(e)
                raise e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            yield df, save_file

def transform_public_cases_ontario_confirmed_positive_cases():
    try: 
        for df, save_file in transform(
            data_in={'classification':'public', 'source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv'}, 
            data_out={'classification':'public', 'source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

@bp.cli.command('isha')
def transform_public_cases_canada_confirmed_positive_cases():
    try: 
        for df, save_file in transform(
            data_in={'classification':'public', 'source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv'},
            data_out={'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_cases_canada_confirmed_mortality_cases():
    try:
        for df, save_file in transform(
            data_in={'classification':'public', 'source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv'},
            data_out={'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_mortality_cases',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_cases_canada_recovered_aggregated():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'canada_recovered_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_cases_international_cases_aggregated():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_cases_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_cases_international_mortality_aggregated():
    try:
        for df in transform(
            data_in = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_mortality_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_cases_international_recovered_aggregated():
    try:
        for df in transform(
            data_in = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_recovered_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_testing_international_testing_aggregated():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'testing', 'table_name':'international_testing_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_testing_canada_testing_aggregated():
    try:
        for df in transform(
            data_in = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'testing', 'table_name':'canada_testing_aggregated',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_interventions_canada_non_pharmaceutical_interventions():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'interventions', 'table_name':'canada_non_pharmaceutical_interventions',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_interventions_international_non_pharmaceutical_interventions():
    try:
        for df in transform(
            data_in = {'classification':'public', 'source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'interventions', 'table_name':'international_non_pharmaceutical_interventions',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_mobility_apple():
    try:
        for df in transform(
            data_in = {'classification':'public', 'source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'interventions', 'table_name':'international_non_pharmaceutical_interventions',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_mobility_google():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'mobility', 'table_name':'google',  'type': 'csv'}):
            df.to_csv(save_file, index=False)
    except Exception as e
        return e

@bp.cli.command('public')
def transform_confidential_moh_iphis():
    try:
        for df, save_file in transform(
            data_in = {'classification':'restricted', 'source_name':'moh', 'table_name':'iphis',  'type': 'csv'},
            data_out = {'classification':'confidential', 'source_name':'moh', 'table_name':'iphis',  'type': 'csv'}):

            for column in ["case_reported_date", "client_death_date"]:
                df[column] = pd.to_datetime(df[column])
            cases = df.groupby(['fsa','case_reported_date']).pseudo_id.count().reset_index()
            cases.rename(columns={'pseudo_id':'cases'},inplace=True)
            cases.rename(columns={'case_reported_date':'date'},inplace=True)
            deaths = df.groupby(['fsa','client_death_date']).pseudo_id.count().reset_index()
            deaths.rename(columns={'pseudo_id':'deaths'},inplace=True)
            deaths.rename(columns={'client_death_date':'date'},inplace=True)
            first_date = min(min(cases["date"]),min(deaths["date"]))
            last_date = max(max(cases["date"]),max(deaths["date"]))
            date_list = [first_date + timedelta(days=x) for x in range((last_date-first_date).days+1)]
            combined_df = pd.merge(cases, deaths, on=['date','fsa'],how="outer")
            combined_df.fillna(0, inplace=True)
            headers = ["date", "fsa", "cases", "deaths"]
            rows = []
            for fsa in set(combined_df["fsa"].values):
                for day in date_list:
                    temp = combined_df[combined_df["fsa"]==fsa]
                    row = temp[temp["date"] == day]
                    if len(row) == 0:
                        rows.append([day, fsa, 0.0, 0.0])
                    else:
                        rows.append([day, fsa, row["cases"].values[0], row["deaths"].values[0]])
            combined_df = pd.DataFrame(rows, columns=headers)
            combined_df.sort_values("date", inplace=True)
            combined_df["cumulative_deaths"] = combined_df.groupby('fsa')['deaths'].cumsum()
            combined_df["cumulative_cases"] = combined_df.groupby('fsa')['cases'].cumsum()
            combined_df.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_socioeconomic_ontario_211_call_reports():
    try:
        for df, save_file in transform(
            data_in = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_reports',  'type': 'csv'}):
            ont_data = df
            ont_data["call_date_and_time_start"] = ont_data["call_date_and_time_start"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['call_date_and_time_start']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            day_count = {}
            for index,row in ont_data.iterrows():
                date = row['call_date_and_time_start']
                callid = row['call_report_num']
                day_count[date] = day_count.get(date,[])+[callid]
            ont_data = ont_data.drop_duplicates(subset='call_report_num', keep="first")
            ont_data = ont_data.groupby(['call_date_and_time_start']).count()
            ont_data = ont_data[['call_report_num']]
            ont_data['call_report_num_7_day_rolling_average'] = ont_data.rolling(window=7).mean()
            ont_data = ont_data.reset_index()
            ont_data.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_socioeconomic_ontario_211_call_reports_by_age():
    try:
        for df, save_file in transform(
            data_in = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_reports_by_age',  'type': 'csv'}):
            ont_data = df
            ont_data["call_date_and_time_start"] = ont_data["call_date_and_time_start"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['call_date_and_time_start']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            ont_data = ont_data.groupby(['call_date_and_time_start', 'age_of_inquirer']).count().reset_index()
            ont_data.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_socioeconomic_ontario_211_call_per_type_of_need():
    try:
        for df, save_file in transform(
            data_in = {'classification':'confidential', 'source_name':'211', 'table_name':'met_and_unmet_needs',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_per_type_of_need',  'type': 'csv'}):
            ont_data = df
            ont_data["date_of_call"] = ont_data["date_of_call"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['date_of_call']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            ont_data = ont_data.drop_duplicates()
            ont_data = ont_data.groupby(['date_of_call', 'airs_need_category']).count()
            ont_data.reset_index(inplace=True)
            ont_data.to_csv(save_file, index=False)
    except Exception as e
        return e

def transform_public_capacity_ontario_lhin_icu_capacity():
    data = {'classification':'restricted', 'source_name':'ccso', 'table_name':'ccis',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    df = pd.read_sql_table('icucapacity', db.engine)
    maxdate = df.iloc[df['date'].idxmax()]['date']
    files = glob.glob(load_dir+"/*."+data['type'])
    fields = ['region', 'lhin', 'critical_care_beds', 'critical_care_patients', 'vented_beds', 'vented_patients', 'confirmed_positive', 'confirmed_positive_ventilator']
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        date = datetime.strptime(date, '%Y-%m-%d')
        if date > maxdate:
            df = pd.read_csv(file)
            df = df.loc[(df.icu_type != 'Neonatal') & (df.icu_type != 'Paediatric')]
            df = df.groupby(['region','lhin']).sum().reset_index()
            for index,row in df.iterrows():
                c = ICUCapacity(date=date)
                for header in fields:
                    setattr(c,header,row[header])
                db.session.add(c)
                db.session.commit()
    df = pd.read_sql_table('icucapacity', db.engine)
    data_out = {'classification':'public', 'source_name':'capacity', 'table_name':'ontario_lhin_icu_capacity',  'type': 'csv'}
    save_file, save_dir = get_file_path(data_out, 'transformed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def transform_public_rt_canada_bettencourt_and_ribeiro_approach():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}):
            cases_df = df
            cases_df = cases_df.loc[cases_df.province == 'Ontario']
            cases_df['date_report'] = pd.to_datetime(cases_df['date_report'])
            province_df = cases_df.groupby(['province', 'date_report'])['case_id'].count()
            province_df.index.rename(['health_region', 'date_report'], inplace=True)
            hr_df = cases_df.groupby(['health_region', 'date_report'])['case_id'].count()
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
            for reg, cases in canada_df.groupby(level='health_region'):
                if cases.max() >= 30:
                    target_regions.append(reg)
            provinces_to_process = canada_df.loc[target_regions]

            results = None
            for prov_name, cases in provinces_to_process.groupby(level='health_region'):
                new, smoothed = prepare_cases(cases)
                try:
                    posteriors = get_posteriors(smoothed)
                except Exception as e:
                    print(e)
                    continue
                hdis = highest_density_interval(posteriors)
                most_likely = posteriors.idxmax().rename('ML')
                result = pd.concat([most_likely, hdis], axis=1).reset_index(level=['health_region', 'date_report'])
                if results is None:
                    results = result
                else:
                    results = results.append(result)

            results['PHU'] = results['health_region']
            results.to_csv(save_file, index=False)

@bp.cli.command('r')
def transform_public_rt_canada_cori_approach():
    data = {'classification':'public', 'source_name':'rt', 'table_name':'canada_cori_approach',  'type': 'csv'}
    load_file, load_dir = get_file_path(data, 'processed')
    load_file = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv"
    save_file, save_dir = get_file_path(data, 'transformed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    out = subprocess.check_output(f"Rscript.exe --vanilla C:/HMF/flattening-the-curve-backend/app/tools/r/Rt_ontario.r {load_file} {save_file}")

def transform_public_capacity_ontario_phu_icu_capacity():

    replace = {
    'Lakeridge Health Corporation':"Durham",
    "Alexandra Hospital":"Southwestern",
    "Alexandra Marine and General Hospital":"Huron Perth",
    "Sunnybrook Health Sciences Centre":"City of Toronto",
    "Quinte Healthcare Corporation":"Hastings and Prince Edward Counties",
    "Scarborough Health Network":"City of Toronto",
    "William Osler Health Centre":"Peel",
    "Brant Community Healthcare System":"Brant County",
    "Cambridge Memorial Hospital":"Region of Waterloo",
    "Brockville General Hospital":"Leeds, Grenville and Lanark District Health Unit",
    "St. Joseph's Healthcare System":"City of Hamilton",
    "Chatham-Kent Health Alliance":"Chatham-Kent",
    "London Health Sciences Centre":"Middlesex-London",
    "Children's Hospital of Eastern Ontario - Ottawa Children's Treatment Centre":"Ottawa Public Health",
    "The Ottawa Hospital":"Ottawa Public Health",
    "Collingwood General and Marine Hospital":"Simcoe Muskoka District Health Unit",
    "Erie Shores Healthcare":"Windsor-Essex County Health Unit",
    "Hamilton Health Sciences":"City of Hamilton",
    "Halton Healthcare Services Corporation":"Halton Region Health Department",
    "Georgian Bay General Hospital":"Simcoe Muskoka District Health Unit",
    "Perth and Smiths Falls District Hospital":"Leeds, Grenville and Lanark District Health Unit",
    "Niagara Health System":"Niagara Region Public Health Department",
    "Guelph General Hospital":"Wellington-Dufferin-Guelph Health Unit",
    "Hawkesbury and District General Hospital":"Eastern Ontario Health Unit",
    "Hopital Montfort":"Ottawa Public Health",
    "Peterborough Regional Health Centre":"Peterborough County-City Health Unit",
    "Muskoka Algonquin Healthcare":"Simcoe Muskoka District Health Unit",
    "Joseph Brant Hospital":"Halton Region Health Department",
    "Kingston Health Sciences Centre":"Kingston, Frontenac and Lennox and Addington Health Unit",
    "Kirkland and District Hospital":"Timiskaming Health Unit",
    "Grand River Hospital Corporation":"Region of Waterloo",
    "Lake-of-the-Woods District Hospital":"Northwestern Health Unit",
    "Lennox and Addington County General Hospital":"Kingston, Frontenac and Lennox and Addington Health Unit",
    "Mackenzie Health":"York Region",
    "Markham Stouffville Hospital":"York Region",
    "Cornwall Community Hospital":"Eastern Ontario Health Unit",
    "Windsor Regional Hospital":"Windsor-Essex County Health Unit",
    "Toronto East Health Network":"City of Toronto",
    "Trillium Health Partners":"Peel",
    "Sinai Health System":"City of Toronto",
    "Norfolk General Hospital":"Haldimand-Norfolk Health Unit",
    "North Bay Regional Health Centre":"North Bay Parry Sound District Health Unit",
    "North York General Hospital":"City of Toronto",
    "Northumberland Hills Hospital":"Haliburton, Kawartha, Pine Ridge District Health Unit",
    "Headwaters Health Care Centre":"Wellington-Dufferin-Guelph Health Unit",
    "Orillia Soldiers Memorial Hospital":"Simcoe Muskoka District Health Unit",
    "Grey Bruce Health Services":"Grey Bruce Health Unit",
    "Pembroke Regional Hospital Inc.":"Renfrew County and District Health Unit",
    "Queensway - Carleton Hospital":"Ottawa Public Health",
    "Health Sciences North":"Sudbury and District Health Unit",
    "Renfrew Victoria Hospital":"Renfrew County and District Health Unit",
    "Ross Memorial Hospital":"Haliburton, Kawartha, Pine Ridge District Health Unit",
    "Royal Victoria Regional Health Centre":"Simcoe Muskoka District Health Unit",
    "Bluewater Health":"Lambton Public Health",
    "Sault Area Hospital":"The District of Algoma Health Unit",
    "Sensenbrenner Hospital":"Porcupine Health Unit",
    "Southlake Regional Health Centre":"York Region Public Health",
    "Unity Health Toronto":"City of Toronto",
    "St. Joseph's General Hospital":"City of Toronto",
    "St. Mary's General Hospital":"Region of Waterloo",
    "St. Thomas-Elgin General Hospital":"Southwestern Public Health",
    "Stevenson Memorial Hospital":"Simcoe Muskoka District Health Unit",
    "Stratford General Hospital":"Huron Perth",
    "Strathroy Middlesex General Hospital":"Middlesex-London",
    "Temiskaming Hospital":"Timiskaming Health Unit",
    "The Hospital for Sick Children":"City of Toronto",
    "Thunder Bay Regional Health Sciences Centre":"Thunder Bay District Health Unit",
    "Tillsonburg District Memorial Hospital":"Southwestern",
    "Timmins and District General Hospital":"Porcupine Health Unit",
    "University Health Network":"City of Toronto",
    "University of Ottawa Heart Institute":"Ottawa Public Health",
    "West Parry Sound Health Centre":"North Bay Parry Sound District Health Uni",
    "Humber River Regional Hospital":"City of Toronto",
    "Woodstock General Hospital":"Southwestern",

    }

    try:
        for df, save_file in transform(
            data_in = {'classification':'restricted', 'source_name':'ccso', 'table_name':'ccis',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'capacity', 'table_name':'ontario_phu_icu_capacity',  'type': 'csv'}):
            df = df.loc[(df.icu_type != 'Neonatal') & (df.icu_type != 'Paediatric')]
            df['phu'] = df['hospital_name'].replace(replace)
            df = df.groupby(['phu']).sum().reset_index()
            df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']
            df.to_csv(save_file, index=False)
    except Exception as e
        pass

def transform_public_cases_ontario_phu_confirmed_positive_aggregated():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'ontario_phu_confirmed_positive_aggregated',  'type': 'csv'}):
            dfs = df.loc[dfs.province == "Ontario"]
            for column in ['date_report']:
                dfs[column] = pd.to_datetime(dfs[column])
            health_regions = dfs.health_region.unique()

            data_frame = {'date_report':[], 'health_regions':[], 'new_cases':[], 'cumulative_cases': []}
            min = dfs['date_report'].min()
            max = dfs['date_report'].max()
            idx = pd.date_range(min, max)

            for region in health_regions:
                df = dfs.loc[dfs.health_region == region]
                df = df.groupby("date_report").case_id.count()
                df = df.reindex(idx, fill_value=0).reset_index()
                date = datetime.strptime("2020-02-28","%Y-%m-%d")
                df = df.loc[df['index'] > date]
                df['date_str'] = df['index'].astype(str)
                df['cumulative_cases'] = df['case_id'].cumsum()

                data_frame['date_report'] += df['index'].tolist()
                data_frame['health_regions'] += [region]*len(df['index'].tolist())
                data_frame['new_cases'] += df['case_id'].tolist()
                data_frame['cumulative_cases'] += df['cumulative_cases'].tolist()

            df_final = pd.DataFrame(data_frame, columns=['date_report', 'health_regions', 'new_cases','cumulative_cases'])
            df_final.to_csv(save_file, index=False)
    except Exception as e
        pass

def transform_public_cases_ontario_phu_weekly_new_cases():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'cases', 'table_name':'ontario_phu_weekly_new_cases',  'type': 'csv'}):

            ont_data = df
            for column in ['case_reported_date']:
                ont_data[column] = pd.to_datetime(ont_data[column])

            start_date = datetime(2020,1,1)
            phus = {}

            for phu in set(ont_data["reporting_phu"].values):
                tmp_data = ont_data[ont_data["reporting_phu"]==phu]
                tmp_data = tmp_data.groupby(['case_reported_date']).count()
                tmp_data = tmp_data[['row_id']]
                tmp_data = tmp_data.reset_index()
                tmp_data['date_week'] = tmp_data['case_reported_date'].apply(lambda x: x.isocalendar()[1])
                tmp_data['date_year'] = tmp_data['case_reported_date'].apply(lambda x: x.isocalendar()[0])
                #ont_data

                week_count_dict = {}
                for dtime in rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=datte.today()):
                    dt_week = dtime.isocalendar()[1]
                    dt_year = dtime.isocalendar()[0]
                    d = f'{dt_year}-W{dt_week-1}'
                    r = datetime.strptime(d + '-1', "%Y-W%W-%w")
                    week_count_dict[r] = sum(tmp_data[(tmp_data["date_year"]==dt_year)&(tmp_data["date_week"]==dt_week)]["row_id"].values)

                    #print(r, week_count_dict[r])
                phus[phu] = week_count_dict

            tmp_data = ont_data
            tmp_data = tmp_data.groupby(['case_reported_date']).count()
            tmp_data = tmp_data[['row_id']]
            tmp_data = tmp_data.reset_index()
            tmp_data['date_week'] = tmp_data['case_reported_date'].apply(lambda x: x.isocalendar()[1])
            tmp_data['date_year'] = tmp_data['case_reported_date'].apply(lambda x: x.isocalendar()[0])
            #ont_data

            week_count_dict = {}
            for dtime in rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=datte.today()):
                dt_week = dtime.isocalendar()[1]
                dt_year = dtime.isocalendar()[0]
                d = f'{dt_year}-W{dt_week-1}'
                r = datetime.strptime(d + '-1', "%Y-W%W-%w")
                week_count_dict[r] = sum(tmp_data[(tmp_data["date_year"]==dt_year)&(tmp_data["date_week"]==dt_week)]["row_id"].values)

                #print(r, week_count_dict[r])
            phus["Ontario"] = week_count_dict
            phu_weekly = pd.DataFrame(phus)
            phu_weekly = phu_weekly.reset_index()
            phu_weekly = pd.melt(phu_weekly, id_vars=['index'])
            phu_weekly = phu_weekly.rename(columns={"index": "Date", "variable": "PHU", "value": "Cases"})
            phu_weekly.to_csv(save_file, index=False)
    except Exception as e
        pass

@bp.cli.command('phu')
def transform_public_capacity_ontario_testing_24_hours():
    try:
        for df, save_file in transform(
            data_in = {'classification':'public', 'source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'},
            data_out = {'classification':'public', 'source_name':'capacity', 'table_name':'ontario_testing_24_hours',  'type': 'csv'}):
            ont_data = df
            for column in ['case_reported_date','specimen_reported_date', 'test_reported_date']:
                ont_data[column] = pd.to_datetime(ont_data[column])
            phus = set(ont_data["reporting_phu"].values)
            #phus = ['Toronto Public Health']
            # output array
            output_arrays = {}

            # for each day in year from start date
            start_date = datte(2020, 5, 1)
            end_date = datte.today()
            delta = timedelta(days=1)
            while start_date <= end_date:
                #print (start_date.strftime("%Y-%m-%d"))
                for phu in phus:
                    tmp = ont_data[(ont_data["reporting_phu"]==phu)&(ont_data["case_reported_date"]==np.datetime64(start_date))]
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
            final_dataframe.to_csv(save_file, index=False)
    except Exception as e
        pass