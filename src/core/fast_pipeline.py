from src.utils.parquet_reader import ParquetReader
from src.view.webviewer import Visualizer


class Pipeline:
    def __init__(self, data_path, strategies, filter_settings):
        self.data_path = data_path
        self.strategies = strategies if isinstance(strategies, list) else [strategies]
        self.filter = filter_settings  # Enthält start_date, end_date, session etc.
        self.raw_data = None
        self.final_data = None

    def load_data(self):
        print(f"--- Loading Data: {self.filter['start_date']} bis {self.filter['end_date']} ---")
        reader = ParquetReader(self.data_path)
        # Wir laden die Daten Eager in den RAM (einmalig)
        self.raw_data = reader.load_range(
            start=self.filter.get("start_date"),
            end=self.filter.get("end_date")
        )
        return self

    def execute_pipeline(self):
        if self.raw_data is None:
            return self
        master_df = self.raw_data

        for strat in self.strategies:
            print(f"-> Processing Strategy: {strat.name}")

            # begin lazy
            # In strat.run_strategy passiert: df.lazy() -> prepare -> filter -> logic -> collect()
            processed_df = strat.run_strategy(master_df, self.filter)

            # PnL Logging
            if strat.pnl_col in processed_df.columns:
                # Bei Polars: .tail(1) oder .item() nutzen für Skalare
                final_pnl = processed_df[strat.pnl_col].tail(1).item()
                print(f"   {strat.name} beendet. PnL: {final_pnl:.2f} USD")

            # Für die Visualisierung speichern wir das Ergebnis der letzten Strategie
            # oder kombinieren sie (je nach Wunsch)
            self.final_data = processed_df

        return self

    def visualize(self):
        """Übergibt das Ergebnis der Strategie-Ausführung an den Visualizer."""
        if self.final_data is not None:
            print("--- Starte Visualisierung ---")
            viz = Visualizer(self.final_data)
            viz.show(symbol="BTC/USD")
        else:
            print("!!! Fehler: Keine Daten für Visualisierung vorhanden.")
        return self

    def run(self):
        """Führt die gesamte Kette aus."""
        (self.load_data()
         .execute_pipeline()
         .visualize())