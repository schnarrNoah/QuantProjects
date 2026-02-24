import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import os


def plot_modern_backtest(df_chart, df_perf, total_candles, start_bal, end_bal, pair="BTC/USD"):
    app = dash.Dash(__name__)
    template_path = os.path.join(os.path.dirname(__file__), "layout.html")

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            app.index_string = f.read()
    except FileNotFoundError:
        print(f"WARNUNG: {template_path} nicht gefunden. Nutze Standard-Template.")

    # Berechnungen für die Metriken (nur einmalig beim Start)
    net_profit = end_bal - start_bal
    roi_percent = (net_profit / start_bal) * 100

    app.layout = html.Div([
        dcc.Tabs(id="tabs-nav", value='tab-perf', children=[
            dcc.Tab(label='STRATEGY METRICS', value='tab-perf'),
            dcc.Tab(label='CHART ANALYSIS', value='tab-chart'),
        ]),
        html.Div(id='tabs-content-area')
    ])

    @app.callback(
        Output('tabs-content-area', 'children'),
        Input('tabs-nav', 'value')
    )
    def render_content(tab):
        if tab == 'tab-chart':
            # Der Chart bekommt die vorgefilterten df_chart Daten
            fig = go.Figure(data=[go.Candlestick(
                x=df_chart['t'],
                open=df_chart['o'], high=df_chart['h'],
                low=df_chart['l'], close=df_chart['c'],
                name="Price"
            )])
            fig.update_layout(
                title=f"{pair} - Letzte {len(df_chart)} Kerzen",
                xaxis_rangeslider_visible=False,
                height=600,
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            return dcc.Graph(figure=fig)

        elif tab == 'tab-perf':
            fig_pnl = go.Figure()
            # HIER DER TRICK: Scattergl nutzt die Grafikkarte (GPU) im Browser!
            fig_pnl.add_trace(
                go.Scattergl(
                    x=df_perf['t'],
                    y=df_perf['pnl_curve'],
                    name="Equity Curve",
                    line=dict(color="#BD0404", width=2)
                )
            )
            fig_pnl.update_layout(
                title="Kontoentwicklung",
                template="plotly_white",
                height=500
            )

            return html.Div([
                html.Div([
                    html.H3("Backtest Results", style={'margin': '0 0 10px 0'}),
                    html.P(f"Analysierte Kerzen: {total_candles:,}"),
                    html.P(f"Startkapital: {start_bal:.2f} USD"),
                    html.H4(f"Net Profit: {net_profit:.2f} USD ({roi_percent:.2f}%)",
                            style={'color': 'green' if net_profit > 0 else 'red'})
                ], style={'padding': '20px', 'background': 'white', 'borderRadius': '5px', 'marginBottom': '20px',
                          'border': '1px solid #ddd'}),

                dcc.Graph(figure=fig_pnl)
            ], style={'padding': '20px'})

    print("Dashboard bereit unter http://127.0.0.1:8050")
    app.run(debug=False)