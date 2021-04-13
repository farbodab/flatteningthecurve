import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from app import db, cache

intervention_categories = ['S1 School Closing',
                           'S2 Workplace closing',
                           'S3 Cancel public events',
                           'S4 Close public transport',
                           'S5 Public info campaigns',
                           'S6 Restrictions on internal movements',
                           'S7 International travel controls',
                           'S8 Fiscal measures',
                           'S9 Monetary measures (interest rate)',
                           'S10 Emergency investment in health care',
                           'S11 Investment in vaccines',
                           'S12 Testing policy',
                           'S13 Contact tracing']

prov_list = ['Ontario', 'Quebec', 'British Columbia', 'Saskatchewan',
             'New Brunswick', 'Nova Scotia', 'Manitoba', 'Alberta',
             'Prince Edward Island', 'Northwest Territories', 'Newfoundland and Labrador', 'Yukon']

PHU = {'The District of Algoma Health Unit':'Algoma Public Health Unit',
 'Brant County Health Unit':'Brant County Health Unit',
 'Durham Regional Health Unit':'Durham Region Health Department',
 'Grey Bruce Health Unit':'Grey Bruce Health Unit',
 'Haldimand-Norfolk Health Unit':'Haldimand-Norfolk Health Unit',
 'Haliburton, Kawartha, Pine Ridge District Health Unit':'Haliburton, Kawartha, Pine Ridge District Health Unit',
 'Halton Regional Health Unit':'Halton Region Health Department',
 'City of Hamilton Health Unit':'Hamilton Public Health Services',
 'Hastings and Prince Edward Counties Health Unit':'Hastings and Prince Edward Counties Health Unit',
 'Huron Perth Public Health Unit':'Huron Perth District Health Unit',
 'Chatham-Kent Health Unit':'Chatham-Kent Health Unit',
 'Kingston, Frontenac, and Lennox and Addington Health Unit':'Kingston, Frontenac and Lennox & Addington Public Health',
 'Lambton Health Unit':'Lambton Public Health',
 'Leeds, Grenville and Lanark District Health Unit':'Leeds, Grenville and Lanark District Health Unit',
 'Middlesex-London Health Unit':'Middlesex-London Health Unit',
 'Niagara Regional Area Health Unit':'Niagara Region Public Health Department',
 'North Bay Parry Sound District Health Unit':'North Bay Parry Sound District Health Unit',
 'Northwestern Health Unit':'Northwestern Health Unit',
 'City of Ottawa Health Unit':'Ottawa Public Health',
 'Peel Regional Health Unit':'Peel Public Health',
 'Huron Perth Public Health Unit':'Huron Perth District Health Unit',
 'Peterborough County–City Health Unit':'Peterborough Public Health',
 'Porcupine Health Unit':'Porcupine Health Unit',
 'Renfrew County and District Health Unit':'Renfrew County and District Health Unit',
 'The Eastern Ontario Health Unit':'Eastern Ontario Health Unit',
 'Simcoe Muskoka District Health Unit':'Simcoe Muskoka District Health Unit',
 'Sudbury and District Health Unit':'Sudbury & District Health Unit',
 'Thunder Bay District Health Unit':'Thunder Bay District Health Unit',
 'Timiskaming Health Unit':'Timiskaming Health Unit',
 'Waterloo Health Unit':'Region of Waterloo, Public Health',
 'Wellington-Dufferin-Guelph Health Unit':'Wellington-Dufferin-Guelph Public Health',
 'Windsor-Essex County Health Unit':'Windsor-Essex County Health Unit',
 'York Regional Health Unit':'York Region Public Health Services',
 'Southwestern Public Health Unit':'Southwestern Public Health',
 'City of Toronto Health Unit':'Toronto Public Health',
 'Ontario': 'Ontario'}

POP = {
     "Algoma": "Algoma Public Health Unit",
     "Brant": "Brant County Health Unit",
     "Chatham-Kent": "Chatham-Kent Health Unit",
     "Durham":"Durham Region Health Department",
     "Eastern":"Eastern Ontario Health Unit",
     "Grey Bruce":"Grey Bruce Health Unit",
     "Haldimand-Norfolk":"Haldimand-Norfolk Health Unit",
     "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Region Health Department",
     "Hamilton":"Hamilton Public Health Services",
     "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron Perth District Health Unit",
     "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac and Lennox & Addington Public Health",
     "Lambton":"Lambton Public Health",
     "Leeds Grenville and Lanark":"Leeds, Grenville and Lanark District Health Unit",
     "Middlesex-London":"Middlesex-London Health Unit",
     "Niagara":"Niagara Region Public Health Department",
     "North Bay Parry Sound":"North Bay Parry Sound District Health Unit",
     "Northwestern":"Northwestern Health Unit",
     "Ottawa":"Ottawa Public Health",
     "Peel":"Peel Public Health",
     "Peterborough":"Peterborough Public Health",
     "Porcupine":"Porcupine Health Unit",
     "Renfrew":"Renfrew County and District Health Unit",
     "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
     "Southwestern":"Southwestern Public Health",
     "Sudbury":"Sudbury & District Health Unit",
     "Thunder Bay":"Thunder Bay District Health Unit",
     "Timiskaming":"Timiskaming Health Unit",
     "Toronto":"Toronto Public Health",
     "Waterloo":"Region of Waterloo, Public Health",
     "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Public Health",
     "Windsor-Essex":"Windsor-Essex County Health Unit",
     "York":"York Region Public Health Services",
 }

def parse_cad(string):
    if type(string) == float:
        return string
    cad = string[1:]
    cad = cad.split('.')[0]
    cad = ''.join(cad.split(','))
    try:
        return float(cad)
    except:
        return 0


def parse_rate(string):
    if type(string) == float:
        return string
    cad = string[:-1]
    try:
        return float(cad)
    except:
        return 0


def impute_intervention(prov):
    for interv_cat in intervention_categories:
        prov[interv_cat] = 0
    closure_geo = ['S1', 'S2', 'S3', 'S4', 'S6']
    public_geo = ['S5']
    travel = ['S7']
    rate = ['S9']
    fiscal = ['S8', 'S10', 'S11']
    test = ['S12']
    trace = ['S13']

    for idx, row in prov.iterrows():
        interv = row['oxford_government_response_category']

        if interv in intervention_categories:
            interv_prefix = str(interv).split(' ')[0]

            subset = prov.iloc[:idx+1]
            subset = subset[subset['oxford_government_response_category'] == interv]

            if interv_prefix in closure_geo:
                prov.at[idx, interv] = (np.nanmax(subset['oxford_closure_code']) + np.nanmax(subset['oxford_geographic_target_code'])) * 100 / 3

            elif interv_prefix in public_geo:
                prov.at[idx, interv] = (np.nanmax(subset['oxford_geographic_target_code']) + np.nanmax(subset['oxford_public_info_code'])) * 100 / 2

            elif interv_prefix in travel:
                prov.at[idx, interv] = (subset['oxford_travel_code'].max()) * 100 / 3

            elif interv_prefix in rate:
                prov.at[idx, interv] = subset['oxford_monetary_measure'].apply(parse_rate).sum()

            elif interv_prefix in fiscal:
                prov.at[idx, interv] = pd.to_numeric(subset['oxford_fiscal_measure_cad']).sum()

            elif interv in test:
                prov.at[idx, interv] = subset['oxford_testing_code'].max() * 100 / 2

            elif interv in trace:
                prov.at[idx, interv] = subset['oxford_tracing_code'].max() * 100 / 2

            if idx > 0:
                for i in intervention_categories:
                    if i != interv:
                        prov.at[idx, i] = prov.at[idx-1, i]
        else:
            if idx > 0:
                for i in intervention_categories:
                    prov.at[idx, i] = prov.at[idx-1, i]

    return prov


def generate_cases_province(pn, pn_short, full_npi):
    prov = full_npi[(full_npi['region'] == pn) & (full_npi['subregion'] != '')]

    prov = prov[['start_date', 'region', 'end_date', 'oxford_government_response_category', 'oxford_closure_code',
       'oxford_public_info_code', 'oxford_travel_code',
       'oxford_geographic_target_code', 'oxford_fiscal_measure_cad',
       'oxford_monetary_measure', 'oxford_testing_code', 'oxford_tracing_code']]

    prov['start_date'] =  pd.to_datetime(prov['start_date'], infer_datetime_format='%m/%d/%Y')
    prov = prov.sort_values(by='start_date',ascending=True).reset_index(drop=True)
    prov['oxford_fiscal_measure_cad'] = prov['oxford_fiscal_measure_cad'].apply(parse_cad)

    prov = impute_intervention(prov)
    prov = prov.drop_duplicates(subset ="start_date",
                     keep = 'last').reset_index(drop=True)

    return prov

interv_string = ['S1 School Closing',
                'S2 Workplace closing',
                'S3 Cancel public events',
                'S4 Close public transport',
                'S5 Public info campaigns',
                'S6 Restrictions on internal movements',
                'S7 International travel controls']

def string_idx(df, date):
    sub = df[df['start_date'] <= date]
    if len(sub):
        sub['stringency'] = sub.apply(lambda r: r[interv_string].fillna(0).values.mean(), axis=1)
        return sub['stringency'].max()
    else:
        return 0


def get_dir(data, today=datetime.today().strftime('%Y-%m-%d')):
    """
    Given variable data (dictionary with classification, stage, source_name
    and table_name) returns custom directory to load file from, along with
    filename which uses today's date by default or custom date
    """
    source_dir = 'data/' + data['classification'] + '/' + data['stage'] + '/'
    load_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_name = data['table_name'] + '_' + today + '.' + data['type']
    file_path =  load_dir + '/' + file_name
    return load_dir, file_path


def get_last_file(data):
    """
    Given variable data (dictionary with classification, stage, source_name
    and table_name) returns the latest file using its date parsed from file
    name
    """
    load_dir, file_path = get_dir(data)
    files = glob.glob(load_dir + "/*." + data['type'])
    files = [file.split('_')[-1] for file in files]
    files = [file.split('.csv')[0] for file in files]
    dates = [datetime.strptime(file, '%Y-%m-%d') for file in files]
    max_date = max(dates).strftime('%Y-%m-%d')
    load_dir, file_path = get_dir(data, max_date)
    return file_path


def get_last(thing):
    if len(thing.dropna()) > 0:
        return thing.dropna().iloc[-1]
    else:
        return np.nan


@cache.memoize(timeout=600)
def get_summary(HR_UID):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1769215322"
    df = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    df['date'] = pd.to_datetime(df['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    df = df.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    if int(HR_UID)==0:
        df = df.loc[df.phu == 'Ontario']
    elif int(HR_UID)>0:
        df = df.loc[df.HR_UID == int(HR_UID)]
    else:
        loop = {"phu":[], "HR_UID":[], "date":[], "rolling":[], "rolling_pop":[], "rolling_pop_trend":[],"rolling_test_twenty_four":[], "rolling_test_twenty_four_trend":[],"confirmed_positive":[], "critical_care_beds":[],"critical_care_patients":[],"critical_care_pct":[], "critical_care_pct_trend":[],"covid_pct":[],"rt_ml":[], "rt_ml_trend":[],"percent_positive": [],"percent_positive_trend": [], "prev": [], "risk": [], "count": []}
        unique = df.HR_UID.unique()
        for hr in unique:
            temp = df.loc[df.HR_UID == hr]
            temp['rolling_pop_trend'] = temp['rolling_pop'].diff(periods=7)
            temp['rt_ml_trend'] = temp['rt_ml'].diff(periods=7)
            temp['rolling_test_twenty_four_trend'] = temp['rolling_test_twenty_four'].diff(periods=7)
            temp['percent_positive_trend'] = temp['% Positivity'].diff(periods=7)
            temp['critical_care_pct_trend']  = temp['critical_care_pct'].diff(periods=7)
            temp['risk'] = temp['risk'].fillna('NaN')
            temp['prev'] = temp['prev'].fillna('NaN')
            if len(temp) > 0:
                loop['phu'].append(get_last(temp['phu']))
                loop['HR_UID'].append(hr)
                loop['date'].append(get_last(temp['date']))
                loop['rolling'].append(get_last(temp['rolling']))
                loop['rolling_pop'].append(get_last(temp['rolling_pop']))
                loop['rolling_pop_trend'].append(get_last(temp['rolling_pop_trend']))
                loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
                loop['rolling_test_twenty_four_trend'].append(get_last(temp['rolling_test_twenty_four_trend']))
                loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
                loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
                loop['covid_pct'].append(get_last(temp['covid_pct']))
                loop['critical_care_pct_trend'].append(get_last(temp['critical_care_pct_trend']))
                loop['rt_ml'].append(get_last(temp['rt_ml']))
                loop['rt_ml_trend'].append(get_last(temp['rt_ml_trend']))
                loop['percent_positive'].append(get_last(temp['% Positivity']))
                loop['percent_positive_trend'].append(get_last(temp['percent_positive_trend']))
                loop['critical_care_beds'].append(get_last(temp['critical_care_beds']))
                loop['critical_care_patients'].append(get_last(temp['critical_care_patients']))
                loop['prev'].append(get_last(temp['prev']))
                loop['risk'].append(get_last(temp['risk']))
                loop['count'].append(get_last(temp['count']))
        temp = df.loc[df.phu == 'Ontario']
        temp['rolling_pop_trend'] = temp['rolling_pop'].diff(periods=7)
        temp['rt_ml_trend'] = temp['rt_ml'].diff(periods=7)
        temp['rolling_test_twenty_four_trend'] = temp['rolling_test_twenty_four'].diff(periods=7)
        temp['percent_positive_trend'] = temp['% Positivity'].diff(periods=7)
        temp['critical_care_pct_trend']  = temp['critical_care_pct'].diff(periods=7)
        temp['risk'] = temp['risk'].fillna('NaN')
        temp['prev'] = temp['prev'].fillna('NaN')
        loop['phu'].append('Ontario')
        loop['HR_UID'].append(-1)
        loop['date'].append(get_last(temp['date']))
        loop['rolling'].append(get_last(temp['rolling']))
        loop['rolling_pop'].append(get_last(temp['rolling_pop']))
        loop['rolling_pop_trend'].append(get_last(temp['rolling_pop_trend']))
        loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
        loop['rolling_test_twenty_four_trend'].append(get_last(temp['rolling_test_twenty_four_trend']))
        loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
        loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
        loop['covid_pct'].append(get_last(temp['covid_pct']))
        loop['critical_care_pct_trend'].append(get_last(temp['critical_care_pct_trend']))
        loop['rt_ml'].append(get_last(temp['rt_ml']))
        loop['rt_ml_trend'].append(get_last(temp['rt_ml_trend']))
        loop['percent_positive'].append(get_last(temp['% Positivity']))
        loop['percent_positive_trend'].append(get_last(temp['percent_positive_trend']))
        loop['critical_care_beds'].append(get_last(temp['critical_care_beds']))
        loop['critical_care_patients'].append(get_last(temp['critical_care_patients']))
        loop['prev'].append(get_last(temp['prev']))
        loop['risk'].append(get_last(temp['risk']))
        loop['count'].append(get_last(temp['count']))
        df = pd.DataFrame(loop)
    return df


def sign_up(email,regions,frequency):
    df = get_summary(-1)
    df = df.round(2)
    temp_df = df.loc[df.HR_UID.isin(regions)]
    regions = temp_df.to_dict(orient='records')
    key = os.environ.get('EMAIL_API')
    sg = sendgrid.SendGridAPIClient(api_key=key)
    from_email = "mycovidreport@howsmyflattening.ca"
    to_email = email
    subject = "Your Personalized COVID-19 Report"
    token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
    html = render_template("welcome_email.html",regions=regions,frequency=frequency, token=token)
    text = render_template("welcome_email.txt",regions=regions,frequency=frequency, token=token)
    message = Mail(
    from_email=from_email,
    to_emails=to_email,
    subject='Your personalized COVID-19 report',
    plain_text_content=text,
    html_content=html)
    try:
        response = sg.send(message)
    except Exception as e:
        print(e.message)


def create_text_response(text):
    response = {
    "fulfillmentText": text,
    "fulfillmentMessages": [
      {
        "text": {
          "text": [
            text
          ]
        }
      }
    ],
    "source": "Howsmyflattening.com"
  }
    return response


def lookup_postal(postal_code):
    postal_code = postal_code.upper().replace(" ", "")
    pccf = pd.read_csv('pccf_on_postal.csv')
    result = pccf.loc[pccf.postal_code == postal_code]
    HR_UID = result['HR_UID'].values[0]
    PHU = result['ENGNAME'].values[0]
    return HR_UID, PHU


def get_fsa(postal_code):
    postal_code = postal_code.upper().replace(" ", "")
    fsa = postal_code[:3]
    df = pd.read_sql_table('iphis', db.engine)
    temp = df.loc[df.fsa == fsa]
    cases = int(temp.tail(1)['cases_two_weeks'].values[0])
    deaths = int(temp.tail(1)['deaths_two_weeks'].values[0])
    date = temp.tail(1)['index'].values[0]
    return date, cases, deaths, fsa


@cache.memoize(timeout=600)
def get_ontario():
    ontario_cases = pd.read_csv("https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv")
    pop = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/hr_map.csv")
    pop = pop.loc[pop.province == "Ontario"]
    pop['health_region'] = pop['health_region'].replace(POP)
    ontario_cases = pd.merge(ontario_cases,pop, left_on=['Reporting_PHU'], right_on=['health_region'], how='left')
    ontario_cases["Case_Reported_Date"] = pd.to_datetime(ontario_cases["Case_Reported_Date"])
    return ontario_cases


@cache.memoize(timeout=600)
def get_alerts():
    df = pd.read_sql_table('alerts', db.engine)
    df = df.loc[df.active == True]
    return df


@cache.memoize(timeout=600)
def get_times():
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1769215322"
    df = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    df['date'] = pd.to_datetime(df['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    df = df.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    df = df.rename(columns={"% Positivity":"percent_positive"})
    df['date'] = df['date'].dt.strftime('%B %d')
    metrics = ["rolling_pop", "rolling_test_twenty_four", "critical_care_pct", "rt_ml", "percent_positive"]
    data = {"rolling_pop":[], "rolling_test_twenty_four":[], "critical_care_pct":[], "rt_ml":[], "percent_positive":[]}
    for metric in metrics:
        temp = df.loc[df[metric].notna()].tail(1)
        date_refreshed = temp['date'].values[:]
        data[metric] = date_refreshed
    return data


@cache.memoize(timeout=600)
def get_vaccination(last=True):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1488620091"
    df = pd.read_csv(url)
    df['date'] = pd.to_datetime(df['date'])
    if last:
        df = df.loc[df.date == df.date.max()]
    return df


@cache.memoize(timeout=600)
def get_risk_df():
    df = pd.read_csv("https://data.ontario.ca/dataset/cbb4d08c-4e56-4b07-9db6-48335241b88a/resource/ce9f043d-f0d4-40f0-9b96-4c8a83ded3f6/download/response_framework.csv")
    phu_mapper={3895:3595,
    5183:3539,
    2244:3544,
    4913:3575,
    2261:3561,
    2268:3568,
    2227:3527,
    2230:3530,
    2258:3558,
    2236:3536,
    2237:3537,
    2246:3546,
    2265:3565,
    2266:3566,
    2270:3570,
    2253:3553,
    2240:3540,
    2233:3533,
    2241:3541,
    2255:3555,
    2262:3562,
    2260:3560,
    2238:3538,
    2242:3542,
    2249:3549,
    2234:3534,
    2235:3535,
    2243:3543,
    2263:3563,
    2226:3526,
    2247:3547,
    2256:3556,
    2257:3557,
    2251:3551
    }
    df['HR_UID'] = df['Reporting_PHU_id'].replace(phu_mapper)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df
