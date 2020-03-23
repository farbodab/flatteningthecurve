from app import db


class CovidDate(db.Model):
    __tablename__ = 'covidDate'
    id = db.Column(db.Integer(), primary_key=True)
    province = db.Column(db.String, index=True)
    date = db.Column(db.DateTime, index=True)


class Covid(db.Model):
    __tablename__ = 'covids'
    id = db.Column(db.Integer(), primary_key=True)
    date = db.Column(db.DateTime, index=True)
    province = db.Column(db.String, index=True)
    count = db.Column(db.Integer)
    hundred = db.Column(db.Integer)

    def from_dict(self, data):
        if 'date' in data:
            self.date = data['date']
        if 'province' in data:
            self.province = data['province']
        if 'count' in data:
            self.count = data['count']
