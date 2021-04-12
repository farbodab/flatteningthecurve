import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta

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
