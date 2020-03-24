from app import db

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
    travel = db.Column(db.Integer())
    travelh = db.Column(db.String(120))

class Comparison(db.Model):
    __tablename__ = 'comparison'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    province = db.Column(db.String, index=True)
    count = db.Column(db.Integer)

class PHUCapacity(db.Model):
    __tablename__ = 'phucapacity'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String, index=True)
    icu = db.Column(db.Integer)
    acute = db.Column(db.Integer)
