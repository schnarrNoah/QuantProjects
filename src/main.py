import sys
from pathlib import Path
from config.config import *
from src.utils.api import API
from src.fast_pipeline import Pipeline
from src.strategy.smc_strad import SMCStrategy

# BASE_DIR ist C:\dev\QuantProjects
BASE_DIR = Path(__file__).resolve().parent

# Der Pfad zur sauberen Parquet-Datei
PARQUET_FILE = Path("C:/dev/QuantProjects/Cryptocurrencies/btc_1min_clean.parquet")
# Der Ordner, in dem die Datei liegt
DATA_DIR = PARQUET_FILE.parent

def run_pipeline():
    my_strategies = [
        SMCStrategy(name="SMC_1to2", rrr=2.0),
        SMCStrategy(name="SMC_1to3", rrr=3.0) #  dieselbe Strategie mit anderen Settings testen!
    ]
    pipe = Pipeline(
        data_path=PARQUET_FILE,
        start_date="2025-01-01T00:00:00",
        end_date="2025-01-31T23:59:59",
        start_time="14:30",
        end_time="16:30",
        session="ny",
        strategies=my_strategies
    )
    pipe.run()

def download_market_candles():
    downloader = API(
        api_key=POLYGON_API_KEY,
        tickers=TICKERS,
        session=SESSION,
        frame=FRAME,
        start=START,
        end=END,
        save_dir=DATA_DIR # Der Downloader speichert meistens in einen Ordner
    )
    downloader.run()

def main():
    # .mkdir() funktioniert jetzt, weil DATA_DIR ein Path-Objekt ist
    # und auf den Ordner zeigt, nicht auf die Datei.
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print("Downloading market data...")
        download_market_candles()
    else:
        print("Running backtest pipeline...")
        run_pipeline()

if __name__ == "__main__":
    main()