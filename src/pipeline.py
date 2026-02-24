from src.process.trader import SMCTrader
from src.utils.csv_reader import CSVReader
from src.view.TabbedViewer import plot_modern_backtest
from src.process.filter_time import *

class Pipeline:
    def __init__(self, data_path, start_date, end_date, start_time, end_time):
        self.data_path = data_path
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.data = None

    def load_data(self):
        reader = CSVReader(self.data_path)
        self.data = reader.load_range(start=self.start_date, end=self.end_date)
        return self

    def filter(self):
        if self.data is not None:
            print(f"--- Filtering: {self.start_time} - {self.end_time} ---")
            self.data = session(self.data)
        return self

    def strategy(self):
        if self.data is not None:
            print(f"--- Strategy: SMC Mitigation (Wait max 10) ---")

            # Instanziiere den Trader
            trader = SMCTrader(initial_balance=10000)

            # Führe die Simulation aus
            self.data = trader.simulate(self.data)

            final_pnl = self.data['pnl_curve'].iloc[-1]
            print(f"Backtest beendet. Final Balance: {final_pnl:.2f} USD")
        return self

    def visualize(self):
        if self.data is not None:
            # Performance-Check: Limit für den Browser
            if len(self.data) > 50000:
                print("WARNUNG: Datensatz zu groß für Browser. Zeige nur letzte 5000 Kerzen.")
                display_data = self.data.tail(5000)
            else:
                display_data = self.data

            plot_modern_backtest(display_data, "BTC/USD")
        return self



    def run(self):
        (self.load_data()
            .filter()
            .strategy()
            .visualize()
        )