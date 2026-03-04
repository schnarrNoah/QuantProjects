from dash import dcc, html, Input, Output, dash
import plotly.graph_objects as go
import socket
import os
import polars as pl
import webbrowser
from threading import Timer


class Visualizer:
    def __init__(self, trading_data: pl.DataFrame):
        self.df = trading_data
        # pathref layout.html, assets
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.layout_path = os.path.join(self.base_path, "layout.html")
        self.assets_path = os.path.join(self.base_path, "assets")

    def _get_free_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()
        return port

    def _prepare_data(self):
        pnl_cols = [col for col in self.df.columns if col.startswith("pnl_")]
        if not pnl_cols:
            raise ValueError("Keine PnL-Spalten gefunden.")

        main_pnl_col = pnl_cols[-1]
        df_plot = self.df.select(["t", "o", "h", "l", "c", main_pnl_col])
        df_plot = df_plot.rename({main_pnl_col: "pnl_curve"})

        total_candles = df_plot.height
        start_bal = df_plot["pnl_curve"].item(0)
        end_bal = df_plot["pnl_curve"].item(-1)

        # Sampling for Performance
        df_chart = df_plot.tail(5000).to_pandas()
        step = max(1, total_candles // 5000)
        df_perf = df_plot.gather_every(step).to_pandas()

        return df_chart, df_perf, total_candles, start_bal, end_bal

    def show(self, symbol="BTC/USD"):
        print("--- Bereite Dashboard vor ---")
        df_chart, df_perf, total_candles, start_bal, end_bal = self._prepare_data()

        app = dash.Dash(__name__, assets_folder=self.assets_path)

        # HTML Template
        try:
            with open(self.layout_path, "r", encoding="utf-8") as f:
                app.index_string = f.read()
            print(f"ERFOLG: layout.html geladen.")
        except FileNotFoundError:
            print(f"!!! FEHLER: layout.html nicht gefunden unter {self.layout_path}")

        # Layout & Callbacks
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

                return html.Div([
                    html.Div([
                        self._create_metric_card("Analysierte Kerzen", f"{total_candles:,}"),
                        self._create_metric_card("Startkapital", f"{start_bal:.2f} USD"),
                        self._create_metric_card("Net Profit / ROI",
                                                 f"{net_profit:.2f} USD ({roi_percent:.2f}%)",
                                                 is_profit=True, val=net_profit),
                    ], className="metric-container"),
                    dcc.Graph(figure=fig_pnl, config={'displayModeBar': False})
                ])
            return None

        port = self._get_free_port()
        url = f"http://127.0.0.1:{port}"

        # auto open
        Timer(1, lambda: webbrowser.open(url)).start()

        print(f"--> Dashboard wird geöffnet unter: {url}")
        app.run(debug=False, port=port)

    def _create_metric_card(self, label, value, is_profit=False, val=0):
        color_class = ""
        if is_profit:
            color_class = "profit-pos" if val >= 0 else "profit-neg"

        return html.Div([
            html.Span(label, className="metric-label"),
            html.Div(value, className=f"metric-value {color_class}")
        ], className="metric-card")