import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import os
import socket

def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def visualize_on_html(df_chart, df_perf, total_candles, start_bal, end_bal, pair="BTC/USD"):
    # ABSOLUTE PFADE (Raw-String wegen Windows Backslashes)
    base_path = r"C:\dev\QuantProjects\src\view"
    layout_path = os.path.join(base_path, "layout.html")
    assets_path = os.path.join(base_path, "assets")

    app = dash.Dash(__name__, assets_folder=assets_path)

    # HTML Template laden
    try:
        with open(layout_path, "r", encoding="utf-8") as f:
            app.index_string = f.read()
        print(f"ERFOLG: layout.html geladen von {layout_path}")
    except FileNotFoundError:
        print(f"!!! FEHLER: layout.html nicht gefunden unter {layout_path}")

    app.layout = html.Div([
        dcc.Tabs(id="tabs-nav", value='tab-perf', children=[
            dcc.Tab(label='STRATEGY METRICS', value='tab-perf'),
            dcc.Tab(label='CHART ANALYSIS', value='tab-chart'),
        ]),
        html.Div(id='tabs-content-area', className="container")
    ])

    @app.callback(
        Output('tabs-content-area', 'children'),
        Input('tabs-nav', 'value')
    )
    def render_content(tab):
        net_profit = end_bal - start_bal
        roi_percent = (net_profit / start_bal) * 100 if start_bal != 0 else 0

        if tab == 'tab-chart':
            fig = go.Figure(data=[go.Candlestick(
                x=df_chart['t'], open=df_chart['o'], high=df_chart['h'],
                low=df_chart['l'], close=df_chart['c'], name="Price"
            )])
            fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, height=600)
            return dcc.Graph(figure=fig)

        elif tab == 'tab-perf':
            fig_pnl = go.Figure()
            fig_pnl.add_trace(go.Scattergl(
                x=df_perf['t'], y=df_perf['pnl_curve'],
                name="Equity Curve",
                line=dict(color="#00ff95" if net_profit >= 0 else "#ff3e3e", width=2)
            ))
            fig_pnl.update_layout(template="plotly_dark", height=500)

            # HIER WAR DER FEHLER: f-string sauber in einer Zeile
            return html.Div([
                html.Div([
                    html.Div([
                        html.Span("Analysierte Kerzen", className="metric-label"),
                        html.Div(f"{total_candles:,}", className="metric-value")
                    ], className="metric-card"),

                    html.Div([
                        html.Span("Startkapital", className="metric-label"),
                        html.Div(f"{start_bal:.2f} USD", className="metric-value")
                    ], className="metric-card"),

                    html.Div([
                        html.Span("Net Profit / ROI", className="metric-label"),
                        html.Div(f"{net_profit:.2f} USD ({roi_percent:.2f}%)",
                                 className=f"metric-value {'profit-pos' if net_profit >= 0 else 'profit-neg'}")
                    ], className="metric-card"),
                ], className="metric-container"),

                dcc.Graph(figure=fig_pnl, config={'displayModeBar': False})
            ])

    active_port = get_free_port()
    print(f"--> Dashboard: http://127.0.0.1:{active_port}")
    app.run(debug=False, port=active_port)