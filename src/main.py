import sys
from pathlib import Path
from config.config import *
from src.utils.api import API
from src.pipeline import Pipeline

# BASE_DIR ist C:\dev\QuantProjects\src^
BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = (BASE_DIR / "../Cryptocurrencies/BTCUSD/1_Minute").resolve()

def run_pipeline():
    pipe = Pipeline(
        data_path=DATA_DIR,
        start="2024-09-10",
        end="2024-09-13"
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
        save_dir=DATA_DIR
    )

    downloader.run()


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print("Downloading market data...")
        download_market_candles()
    else:
        print("Running backtest pipeline...")
        run_pipeline()


if __name__ == "__main__":
    main()
