import sys, os
from pathlib import Path
from src.utils.api import API
from src.core.fast_pipeline import Pipeline
from src.strategy.smc import *
from dotenv import load_dotenv


sys.path.append(str(Path(__file__).resolve().parent.parent))
BASE_DIR = Path(__file__).resolve().parent
#PARQUET_FILE = Path("C:/dev/QuantProjects/Cryptocurrencies/BTCUSD/btc_1min.parquet")
PARQUET_FILE = Path("/Users/n/Python_Apps/QuantProjects/Cryptocurrencies/BTCUSD/btc_1min.parquet")
DATA_DIR = PARQUET_FILE.parent


def run_pipeline():
    settings = {
        "start_date": "2025-01-01T00:00:00",
        "end_date": "2025-12-31T23:59:59",
        "start_time": "14:30",
        "end_time": "16:30",
        "session": "ny" # Die Session, in der GEHANDELT wird
    }

    my_strategies = [
        SMC_NY_Strategy(name="SMC", rrr=1.0),
    ]

    pipe = Pipeline(
        data_path=PARQUET_FILE,
        strategies=my_strategies,
        filter_settings=settings
    )
    pipe.run()


def download_market_candles():
    downloader = API(
        api_key=os.getenv("APIKEY_MASSIVE"),
        tickers=["X:BTCUSD"],
        frame="minute",
        parquet_file=PARQUET_FILE,
        start_fallback="2025-01-01"  # Nur falls die Datei noch gar nicht existiert
    )
    downloader.run()  # Die API kümmert sich jetzt selbst um den Rest!


######################################################


def main():
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / '.env'
    load_dotenv(env_path)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "download":
        api_key = os.getenv("APIKEY_MASSIVE")
        if not api_key:
            print(f"Abbruch: Kein API-Key in {env_path} gefunden.")
            return

        print("Status: Starte Marktdaten-Download...")
        download_market_candles()
    else:
        print("Status: Starte Backtest-Pipeline...")
        run_pipeline()


if __name__ == "__main__":
    main()