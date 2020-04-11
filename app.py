import plotly_express as px
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

from Portfolio import Portfolio

fidelity_holdings_path = 'Portfolio_Position_Apr-04-2020.csv'
etf_holdings_paths = {
    "SPY":'holdings-daily-us-en-spy.xlsx',
    "MTUM":'MTUM_holdings.csv',
    "USMV":'USMV_holdings.csv',
    } 

quandl_api_key = '-zM7wKFJ3BGagyXxkPwX'
date = '2019-12-31'

my_ptf = Portfolio('my_ptf',quandl_api_key)
my_ptf.add_holdings(fidelity_holdings_path,"Fidelity Individual")
my_ptf.clean_tickers()
my_ptf.explode_etfs(etf_holdings_paths)

my_ptf.get_fundamentals(date,'MRT')
my_ptf.get_metadata()
my_ptf.merge_holdings_fundamentals()

fig = px.sunburst(my_ptf.df, path=['sector','industry','ticker'], values='weight',
                 color='weight',color_continuous_scale='RdBu',color_continuous_midpoint=my_ptf.df.weight.mean(),maxdepth=2)
app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)

app.layout = html.Div(
    [
        html.H1("Demo: Plotly Express in Dash with Tips Dataset"),
        html.Div(style={"width": "25%", "float": "left"},
        ),
        dcc.Graph(figure=fig),
    ]
)


app.run_server(debug=True)
