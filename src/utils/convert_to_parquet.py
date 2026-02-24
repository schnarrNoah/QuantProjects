import polars as pl
from pathlib import Path
import tqdm  # Für den Fortschrittsbalken: pip install tqdm

def convert_csv_to_parquet(source_dir, output_file):
    # Sicherstellen, dass die Pfade Path-Objekte sind
    source_path = Path(source_dir)
    output_path = Path(output_file)

    # Alle Dateien finden
    all_files = list(source_path.glob("**/*.csv"))

    if not all_files:
        print(f"Keine CSV-Dateien in {source_dir} gefunden.")
        return

    print(f"Gefunden: {len(all_files)} Dateien. Starte Bereinigung...")

    processed_dfs = []

    for file in tqdm.tqdm(all_files):
        try:
            # Wir lesen jede Datei einzeln und zwingen sie in ein festes Schema
            # infer_schema_length=10000 hilft Polars, die Spalten besser zu raten,
            # bevor wir sie manuell casten.
            df = (
                pl.read_csv(file, try_parse_dates=True, infer_schema_length=10000)
                .with_columns([
                    # Zeitzone UTC hart entfernen für "Naive" Vergleichbarkeit
                    pl.col("t").dt.replace_time_zone(None),
                    # Alle numerischen Werte auf Float32 (spart 50% Platz gegenüber Float64)
                    pl.col(["o", "h", "l", "c", "v", "vw"]).cast(pl.Float32)
                ])
                .select(["t", "o", "h", "l", "c", "v", "vw"])
            )
            processed_dfs.append(df)
        except Exception as e:
            print(f"Fehler in Datei {file.name}: {e}")

    if not processed_dfs:
        print("Keine Daten erfolgreich verarbeitet.")
        return

    # Alles zusammenfügen
    print("Kombiniere Daten und entferne Dubletten...")
    final_df = (
        pl.concat(processed_dfs)
        .unique(subset="t")
        .sort("t")
    )

    # Als Parquet speichern (mit Snappy-Kompression für optimalen Speed/Platz)
    final_df.write_parquet(output_path, compression="snappy")
    print("-" * 30)
    print(f"Erfolg! Saubere Daten gespeichert unter: {output_path}")
    print(f"Gesamtzeilen: {final_df.height:,}")
    print("-" * 30)

if __name__ == "__main__":
    # DEINE PFADE
    DATA_ROOT = "C:/dev/QuantProjects/Cryptocurrencies/BTCUSD/1_Minute"
    TARGET = "C:/dev/QuantProjects/Cryptocurrencies/btc_1min_clean.parquet"

    # Ordner für Target erstellen falls nicht existent
    target_path = Path(TARGET)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    convert_csv_to_parquet(DATA_ROOT, TARGET)