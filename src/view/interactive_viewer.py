import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import pandas as pd


class InteractiveViewer:
    def __init__(self, df, pair="BTC/USD"):
        self.df = df
        self.pair = pair
        self.mode = "normal"  # Start-Modus

        # Erstelle das Hauptfenster
        self.fig, self.ax = plt.subplots(figsize=(14, 8))
        plt.subplots_adjust(bottom=0.2)  # Platz für Buttons unten lassen

        # Buttons hinzufügen
        ax_btn_normal = plt.axes([0.35, 0.05, 0.1, 0.075])
        ax_btn_fvg = plt.axes([0.55, 0.05, 0.1, 0.075])

        self.btn_normal = Button(ax_btn_normal, 'Normal View')
        self.btn_fvg = Button(ax_btn_fvg, 'FVG View')

        self.btn_normal.on_clicked(self.show_normal)
        self.btn_fvg.on_clicked(self.show_fvg)

        # Initialer Plot
        self.update_plot()
        plt.show()

    def update_plot(self):
        self.ax.clear()
        # Basis-Linie (Preis)
        self.ax.plot(self.df.index, self.df["c"], color="black", linewidth=1, alpha=0.7, label="Price")

        if self.mode == "fvg":
            self.draw_fvg()
            self.ax.set_title(f"FVG Monitor - {self.pair}")
        else:
            self.ax.set_title(f"Standard Price View - {self.pair}")

        self.ax.legend()
        self.ax.grid(True, alpha=0.2)
        self.fig.canvas.draw_idle()

    def draw_fvg(self):
        # Bullish
        if 'bull_fvg' in self.df.columns:
            for ts in self.df.index[self.df['bull_fvg']]:
                self.ax.fill_between([ts, ts + pd.Timedelta(minutes=5)],
                                     self.df.at[ts, 'bull_fvg_bottom'],
                                     self.df.at[ts, 'bull_fvg_top'],
                                     color='green', alpha=0.4)
        # Bearish
        if 'bear_fvg' in self.df.columns:
            for ts in self.df.index[self.df['bear_fvg']]:
                self.ax.fill_between([ts, ts + pd.Timedelta(minutes=5)],
                                     self.df.at[ts, 'bear_fvg_bottom'],
                                     self.df.at[ts, 'bear_fvg_top'],
                                     color='red', alpha=0.4)

    def show_normal(self, event):
        self.mode = "normal"
        self.update_plot()

    def show_fvg(self, event):
        self.mode = "fvg"
        self.update_plot()