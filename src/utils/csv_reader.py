import pandas as pd
from pathlib import Path


class CSVReader:
    def __init__(self, base_path):
        self.base_path = Path(base_path)

    def load_range(self, start, end):
        # 1. Zeiträume normalisieren
        start_dt = pd.to_datetime(start).tz_localize(None)
        end_dt = pd.to_datetime(end).tz_localize(None)
        all_dfs = []

        # Jahre ermitteln, die wir tatsächlich brauchen
        start_year = start_dt.year
        end_year = end_dt.year

        print(f"Optimierte Suche in: {self.base_path}")
        print(f"Berücksichtigte Jahre: {list(range(start_year, end_year + 1))}")

        # 2. Nur durch die relevanten Jahres-Ordner iterieren
        for year in range(start_year, end_year + 1):
            year_path = self.base_path / str(year)

            if not year_path.exists():
                print(f"Überspringe Jahr {year}: Ordner nicht vorhanden.")
                continue

            # In diesem Jahr-Ordner alle CSVs suchen
            # glob("*.csv") reicht hier, da wir schon im richtigen Jahr sind
            files = sorted(year_path.glob("*.csv"))

            for file in files:
                try:
                    # Wir nutzen den Dateinamen für einen Vor-Check (optional aber schnell)
                    # "X_BTCUSD_2024-03-01..." -> Falls der Name das Datum enthält

                    df = pd.read_csv(file)
                    df.columns = df.columns.str.strip()

                    if 't' not in df.columns:
                        continue

                    df['t'] = pd.to_datetime(df['t']).dt.tz_localize(None)
                    df.set_index('t', inplace=True)
                    df.sort_index(inplace=True)

                    if not ((df.index.max() < start_dt) or (df.index.min() > end_dt)):
                        mask = (df.index >= start_dt) & (df.index <= end_dt)
                        chunk = df.loc[mask]

                        if not chunk.empty:
                            print(f"Lade: {year}/{file.name}")
                            all_dfs.append(chunk)

                except Exception as e:
                    print(f"Fehler bei {file.name}: {e}")

        if not all_dfs:
            raise ValueError(f"Keine Daten für den Zeitraum {start_dt} bis {end_dt} gefunden!")

        # 3. Finales Zusammenfügen
        result = pd.concat(all_dfs)
        result = result[~result.index.duplicated()].sort_index()

        print(f"Erfolg: {len(result)} Zeilen geladen.")
        return result