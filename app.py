from flask import Flask, render_template
import requests
import plotly.graph_objects as go

app = Flask(__name__)

@app.route('/')
def index():
    r = requests.get("https://ihs-api.herokuapp.com/covid/allc")
    data = r.json()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=list(data["Italy"].keys()), y=list(data["Italy"].values()),
                    mode='lines',
                    name='Italy'))
    fig.add_trace(go.Scatter(x=list(data["Ontario"].keys()), y=list(data["Italy"].values()),
                    mode='lines',
                    name='Ontario'))
    fig.add_trace(go.Scatter(x=list(data["Italy"].keys()), y=list(data["Singapore"].values()),
                    mode='lines',
                    name='Singapore'))
    fig.add_trace(go.Scatter(x=list(data["South Korea"].keys()), y=list(data["South Korea"].values()),
                    mode='lines',
                    name='South Korea'))
    fig.update_layout(
                    autosize=False,
                    width=1000,
                    height=800,
                   xaxis_title='Days after 100 confirmed cases',
                   yaxis_title='ICU beds')
    div = fig.to_html(full_html=True)
    return render_template('index.html', plot=div)

@app.route('/about')
def about():
    return render_template('story.html')
