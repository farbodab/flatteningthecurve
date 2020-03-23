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
    fig.add_trace(go.Scatter(x=list(data["Ontario"].keys()), y=list(data["Ontario"].values()),
                    mode='lines',
                    name='Ontario'))
    fig.add_trace(go.Scatter(x=list(data["Singapore"].keys()), y=list(data["Singapore"].values()),
                    mode='lines',
                    name='Singapore'))
    fig.add_trace(go.Scatter(x=list(data["South Korea"].keys()), y=list(data["South Korea"].values()),
                    mode='lines',
                    name='South Korea'))
    fig.update_layout(
                    autosize=True,
                    width=1200,
                    height=1000,
                   xaxis_title='Days after 100 confirmed cases',
                   yaxis_title='ICU beds',
                   yaxis_range=[0,2000])
    fig.add_shape(
        # Line Horizontal
            type="line",
            x0=0,
            y0=1500,
            x1=30,
            y1=1500,
            line=dict(
                color="LightSeaGreen",
                width=4,
                dash="dashdot",
            ),
    )
    div = fig.to_html(full_html=True)
    return render_template('index.html', plot=div)

@app.route('/about')
def about():
    return render_template('story.html')
