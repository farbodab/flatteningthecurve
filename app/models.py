from app import db


class CasesStatus(db.Model):
    __tablename__ = "casesstatus"
    id = db.Column(db.Integer, primary_key=True)
    reported_date = db.Column(db.DateTime, index=True)
    confirmed_negative = db.Column(db.Integer)
    presumptive_negative = db.Column(db.Integer)
    presumptive_positive = db.Column(db.Integer)
    confirmed_positive = db.Column(db.Integer)
    resolved = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    total_cases = db.Column(db.Integer)
    patients_approved = db.Column(db.Integer)
    tests_today = db.Column(db.Integer)
    under_investigation = db.Column(db.Integer)
    hospitalized = db.Column(db.Integer)
    icu = db.Column(db.Integer)
    icu_ventilator = db.Column(db.Integer)

class CovidTests(db.Model):
    __tablename__ = 'covidtests'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    negative = db.Column(db.Integer)
    investigation = db.Column(db.Integer)
    positive = db.Column(db.Integer)
    resolved = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    total = db.Column(db.Integer)
    hospitalized = db.Column(db.Integer)
    icu = db.Column(db.Integer)
    ventilator = db.Column(db.Integer)

class Covid(db.Model):
    __tablename__ = 'covid'
    id = db.Column(db.Integer(), primary_key=True)
    case_id = db.Column(db.Integer(), index=True)
    age = db.Column(db.String(120))
    sex = db.Column(db.String(120))
    region = db.Column(db.String(120))
    province = db.Column(db.String(120))
    country = db.Column(db.String(120))
    date = db.Column(db.DateTime, index=True)
    travel = db.Column(db.String())
    travelh = db.Column(db.String(120))

class Comparison(db.Model):
    __tablename__ = 'comparison'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    province = db.Column(db.String, index=True)
    count = db.Column(db.Integer)

class InternationalData(db.Model):
    __tablename__ = 'internationaldata'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    country = db.Column(db.String, index=True)
    cases = db.Column(db.Integer)


class PHUCapacity(db.Model):
    __tablename__ = 'phucapacity'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String, index=True)
    icu = db.Column(db.Integer)
    acute = db.Column(db.Integer)


class ICUCapacity(db.Model):
    __tablename__ = 'icucapacity'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    icu_level = db.Column(db.String)
    region = db.Column(db.String)
    lhin = db.Column(db.String)
    critical_care_beds = db.Column(db.Integer)
    critical_care_patients = db.Column(db.Integer)
    vented_beds = db.Column(db.Integer)
    vented_patients = db.Column(db.Integer)
    suspected_covid = db.Column(db.Integer)
    suspected_covid_ventilator = db.Column(db.Integer)
    confirmed_positive = db.Column(db.Integer)
    confirmed_positive_ventilator = db.Column(db.Integer)
    confirmed_negative = db.Column(db.Integer)

class CanadaMortality(db.Model):
    __tablename__ = 'canadamortality'
    id = db.Column(db.Integer(), primary_key=True)
    death_id = db.Column(db.Integer(), index=True)
    province_death_id = db.Column(db.Integer(), index=True)
    age = db.Column(db.String())
    sex = db.Column(db.String())
    region = db.Column(db.String())
    province = db.Column(db.String())
    country = db.Column(db.String())
    date = db.Column(db.DateTime, index=True)
    death_source = db.Column(db.String())

class CanadaRecovered(db.Model):
    __tablename__ = 'canadarecovered'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    province = db.Column(db.String(), index=True)
    cumulative_recovered = db.Column(db.Integer())

class CanadaTesting(db.Model):
    __tablename__ = 'canadatesting'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    province = db.Column(db.String(), index=True)
    cumulative_testing = db.Column(db.Integer())

class InternationalMortality(db.Model):
    __tablename__ = 'internationalmortality'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    country = db.Column(db.String, index=True)
    deaths = db.Column(db.Integer)

class InternationalRecovered(db.Model):
    __tablename__ = 'internationalrecovered'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    country = db.Column(db.String, index=True)
    recovered = db.Column(db.Integer)

class InternationalTesting(db.Model):
    __tablename__ = 'internationaltesting'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    region = db.Column(db.String(), index=True)
    cumulative_testing = db.Column(db.Integer())

class NPIInterventions(db.Model):
    __tablename__ = 'npiintervention'
    id = db.Column(db.Integer(), primary_key=True)
    start_date = db.Column(db.DateTime, index=True)
    end_date = db.Column(db.DateTime, index=True)
    country = db.Column(db.String, index=True)
    region = db.Column(db.String, index=True)
    subregion = db.Column(db.String, index=True)
    intervention_summary = db.Column(db.String, index=True)
    intervention_category = db.Column(db.String)
    target_population_category = db.Column(db.String)
    enforcement_category = db.Column(db.String)
    oxford_government_response_category	= db.Column(db.String)
    oxford_closure_code	= db.Column(db.String)
    oxford_public_info_code	= db.Column(db.String)
    oxford_travel_code	= db.Column(db.String)
    oxford_geographic_target_code	= db.Column(db.String)
    oxford_fiscal_measure_cad = db.Column(db.String)
    oxford_testing_code	= db.Column(db.String)
    oxford_tracing_code	= db.Column(db.String)
    oxford_restriction_code	= db.Column(db.String)
    oxford_income_amount = db.Column(db.String)
    oxford_debt_relief_code = db.Column(db.String)
    source_url = db.Column(db.String)
    source_organization	 = db.Column(db.String)
    source_organization_2 = db.Column(db.String)
    source_category= db.Column(db.String)
    source_title= db.Column(db.String)
    source_full_text = db.Column(db.String)
    note = db.Column(db.String)
    end_source_url = db.Column(db.String)
    end_source_organization	= db.Column(db.String)
    end_source_organization_2 = db.Column(db.String)
    end_source_category	= db.Column(db.String)
    end_source_title = db.Column(db.String)
    end_source_full_text = db.Column(db.String)

class Viz(db.Model):
    __tablename__ = 'viz'
    id = db.Column(db.Integer(), primary_key=True)
    header = db.Column(db.String, index=True)
    category = db.Column(db.String)
    content = db.Column(db.String)
    viz = db.Column(db.String)
    thumbnail = db.Column(db.String)
    text_top = db.Column(db.String)
    text_bottom = db.Column(db.String)
    mobileHeight = db.Column(db.Integer)
    desktopHeight = db.Column(db.Integer)
    page = db.Column(db.String,index=True)
    order = db.Column(db.Integer)
    row = db.Column(db.Integer)
    column = db.Column(db.Integer)
    html = db.Column(db.String)
    phu = db.Column(db.String)
    tab_order = db.Column(db.Integer)
    viz_type = db.Column(db.String)
    viz_title = db.Column(db.String)
    date = db.Column(db.DateTime)
    visible = db.Column(db.Boolean)

class Source(db.Model):
    __tablename__ = 'source'
    id = db.Column(db.Integer(), primary_key=True)
    region = db.Column(db.String, index=True)
    type = db.Column(db.String, index=True)
    name = db.Column(db.String, index=True)
    source = db.Column(db.String)
    description = db.Column(db.String)
    data_feed_type = db.Column(db.String)
    link = db.Column(db.String)
    refresh = db.Column(db.String)
    contributor = db.Column(db.String)
    contact = db.Column(db.String)
    download = db.Column(db.String)

class Member(db.Model):
    __tablename__ = 'members'
    id = db.Column(db.Integer(), primary_key=True)
    team = db.Column(db.String, index=True)
    title = db.Column(db.String)
    first_name = db.Column(db.String,index=True)
    last_name = db.Column(db.String, index=True)
    education = db.Column(db.String)
    affiliation = db.Column(db.String)
    role = db.Column(db.String)
    team_status = db.Column(db.String)
    linkedin = db.Column(db.String)

class Mobility(db.Model):
    __tablename__ = 'mobility'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    region = db.Column(db.String)
    category = db.Column(db.String)
    value = db.Column(db.Float)

class MobilityTransportation(db.Model):
    __tablename__ = 'mobilitytransportation'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    source = db.Column(db.String)
    region = db.Column(db.String)
    transportation_type = db.Column(db.String)
    value = db.Column(db.Float)

class NPIInterventionsUSA(db.Model):
    __tablename__ = 'npiinterventions_usa'
    id = db.Column(db.Integer(), primary_key=True)
    state = db.Column(db.String, index=True)
    county = db.Column(db.String)
    npi = db.Column(db.String, index=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    citation = db.Column(db.String)
    note = db.Column(db.String)

class GovernmentResponse(db.Model):
    __tablename__ = 'npiinterventions_world'
    id = db.Column(db.Integer(), primary_key=True)
    country = db.Column(db.String, index=True)
    country_code = db.Column(db.String)
    date = db.Column(db.DateTime, index=True)
    c1_school_closing = db.Column(db.String)
    c1_flag	= db.Column(db.String)
    c2_workplace_closing = db.Column(db.String)
    c2_flag	= db.Column(db.String)
    c3_cancel_public_events	= db.Column(db.String)
    c3_flag	= db.Column(db.String)
    c4_restrictions_on_gatherings= db.Column(db.String)
    c4_flag	= db.Column(db.String)
    c5_close_public_transport = db.Column(db.String)
    c5_flag	= db.Column(db.String)
    c6_stay_at_home_requirements = db.Column(db.String)
    c6_flag	= db.Column(db.String)
    c7_restrictions_on_internal_movement = db.Column(db.String)
    c7_flag	= db.Column(db.String)
    c8_international_travel_controls = db.Column(db.String)
    e1_income_support = db.Column(db.String)
    e1_flag	= db.Column(db.String)
    e2_debt_contract_relief	= db.Column(db.String)
    e3_fiscal_measures = db.Column(db.String)
    e4_international_support = db.Column(db.String)
    h1_public_information_campaigns	= db.Column(db.String)
    h1_flag	= db.Column(db.String)
    h2_testing_policy = db.Column(db.String)
    h3_contact_tracing = db.Column(db.String)
    h4_emergency_investment_in_healthcare = db.Column(db.String)
    h5_investment_in_vaccines = db.Column(db.String)
    m1_wildcard	= db.Column(db.String)
    confirmed_cases	= db.Column(db.String)
    confirmed_deaths	= db.Column(db.String)
    stringency_index	= db.Column(db.String)
    stringency_index_for_display	= db.Column(db.String)
    stringency_legacy_index	= db.Column(db.String)
    stringency_legacy_index_for_display	= db.Column(db.String)
    government_response_index	= db.Column(db.String)
    government_response_index_for_display	= db.Column(db.String)
    containment_health_index	= db.Column(db.String)
    containment_health_index_for_display	= db.Column(db.String)
    economic_support_index	= db.Column(db.String)
    economic_support_index_for_display = db.Column(db.String)


class LongTermCare(db.Model):
    __tablename__ = 'longtermcare'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    home = db.Column(db.String)
    city = db.Column(db.String)
    beds = db.Column(db.Integer)
    confirmed_resident_cases = db.Column(db.Integer)
    resident_deaths = db.Column(db.Integer)
    confirmed_staff_cases = db.Column(db.Integer)
    phu = db.Column(db.String)

class LongTermCareSummary(db.Model):
    __tablename__ = 'longtermcare_summary'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    report = db.Column(db.String)
    number = db.Column(db.Integer)

class LongTermCareNoLongerInOutbreak(db.Model):
    __tablename__ = 'longtermcare_nolongerinoutbreak'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    home = db.Column(db.String)
    city = db.Column(db.String)
    beds = db.Column(db.Integer)
    resident_deaths = db.Column(db.Integer)
    phu = db.Column(db.String)

class PredictiveModel(db.Model):
    __tablename__ = 'predictivemodel'
    id = db.Column(db.Integer, primary_key=True)
    date_retrieved = db.Column(db.DateTime, index=True)
    region = db.Column(db.String, index=True)
    date = db.Column(db.DateTime, index=True)
    cumulative_incidence = db.Column(db.Integer)
    required_hospW = db.Column(db.Integer)
    required_hospNonVentICU = db.Column(db.Integer)
    required_hospVentICU = db.Column(db.Integer)
    available_hospW = db.Column(db.Integer)
    available_hospNonVentICU = db.Column(db.Integer)
    available_hospVentICU = db.Column(db.Integer)
    waiting_hospW = db.Column(db.Integer)
    waiting_hospNonVentICU = db.Column(db.Integer)
    waiting_hospVentICU = db.Column(db.Integer)

class IDEAModel(db.Model):
    __tablename__ = 'ideamodel'
    id = db.Column(db.Integer, primary_key=True)
    date_retrieved = db.Column(db.DateTime, index=True)
    source = db.Column(db.String, index=True)
    date = db.Column(db.DateTime, index=True)
    reported_cases = db.Column(db.Integer)
    model_incident_cases = db.Column(db.Float)
    model_incident_cases_lower_PI = db.Column(db.Float)
    model_incident_cases_upper_PI = db.Column(db.Float)
    reported_cumulative_cases = db.Column(db.Integer)
    model_cumulative_cases = db.Column(db.Float)
    model_cumulative_cases_lower_PI = db.Column(db.Float)
    model_cumulative_cases_upper_PI = db.Column(db.Float)

class ConfirmedOntario(db.Model):
    __tablename__ = 'confirmedontario'
    id = db.Column(db.Integer, primary_key=True)
    row_id = db.Column(db.Integer, index=True)
    accurate_episode_date = db.Column(db.DateTime, index=True)
    case_reported_date = db.Column(db.DateTime, index=True)
    test_reported_date = db.Column(db.DateTime, index=True)
    specimen_reported_date = db.Column(db.DateTime, index=True)
    age_group = db.Column(db.String)
    client_gender = db.Column(db.String)
    case_acquisitionInfo = db.Column(db.String)
    outcome1 = db.Column(db.String)
    outbreak_related = db.Column(db.String)
    reporting_phu = db.Column(db.String)
    reporting_phu_address = db.Column(db.String)
    reporting_phu_city = db.Column(db.String)
    reporting_phu_postal_code = db.Column(db.String)
    reporting_phu_website = db.Column(db.String)
    reporting_phu_latitude = db.Column(db.String)
    reporting_phu_longitude = db.Column(db.String)
