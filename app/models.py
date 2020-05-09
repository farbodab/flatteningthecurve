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
    __tablename__ = 'npiinterventions'
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
    oxford_monetary_measure	= db.Column(db.String)
    oxford_testing_code	= db.Column(db.String)
    oxford_tracing_code	= db.Column(db.String)
    source_url = db.Column(db.String)
    source_organization	 = db.Column(db.String)
    source_organization_two = db.Column(db.String)
    source_category= db.Column(db.String)
    source_title= db.Column(db.String)
    source_full_text = db.Column(db.String)

class Viz(db.Model):
    __tablename__ = 'viz'
    id = db.Column(db.Integer(), primary_key=True)
    header = db.Column(db.String, index=True)
    category = db.Column(db.String)
    content = db.Column(db.String)
    viz = db.Column(db.String)
    thumbnail = db.Column(db.String)
    text = db.Column(db.String)
    mobileHeight = db.Column(db.Integer)
    desktopHeight = db.Column(db.Integer)
    page = db.Column(db.String,index=True)
    order = db.Column(db.Integer)
    row = db.Column(db.Integer)
    column = db.Column(db.Integer)
    html = db.Column(db.String)
    phu = db.Column(db.String)



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
    __tablename__ = 'governmentresponse'
    id = db.Column(db.Integer(), primary_key=True)
    country = db.Column(db.String, index=True)
    country_code = db.Column(db.String)
    date = db.Column(db.DateTime, index=True)
    s1_school_closing = db.Column(db.Integer)
    s1_is_general = db.Column(db.Integer)
    s1_notes = db.Column(db.String)
    s2_workplace_closing = db.Column(db.Integer)
    s2_is_general = db.Column(db.Integer)
    s2_notes = db.Column(db.String)
    s3_cancel_public_events = db.Column(db.Integer)
    s3_is_general = db.Column(db.Integer)
    s3_notes = db.Column(db.String)
    s4_close_public_transport = db.Column(db.Integer)
    s4_is_general = db.Column(db.Integer)
    s4_notes = db.Column(db.String)
    s5_public_information_campaigns = db.Column(db.Integer)
    s5_is_general = db.Column(db.Integer)
    s5_notes = db.Column(db.String)
    s6_restrictions_on_internal_movement = db.Column(db.Integer)
    s6_is_general = db.Column(db.Integer)
    s6_notes = db.Column(db.String)
    s7_international_travel_controls = db.Column(db.Integer)
    s7_notes = db.Column(db.String)
    s8_fiscal_measures = db.Column(db.BigInteger)
    s8_notes = db.Column(db.String)
    s9_monetary_measures = db.Column(db.Float)
    s9_notes = db.Column(db.String)
    s10_emergency_investment_in_healthcare = db.Column(db.BigInteger)
    s10_notes = db.Column(db.String)
    s11_investement_in_vaccines = db.Column(db.BigInteger)
    s11_notes = db.Column(db.String)
    s12_testing_framework = db.Column(db.Integer)
    s12_notes = db.Column(db.String)
    s13_contact_tracing = db.Column(db.Integer)
    s13_notes = db.Column(db.String)
    confirmed_cases = db.Column(db.Integer)
    confirmed_deaths = db.Column(db.Integer)
    stringency_index = db.Column(db.Float)
    stringency_index_for_display = db.Column(db.Float)

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
