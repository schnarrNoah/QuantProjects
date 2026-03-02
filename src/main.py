import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import *
from src.utils.api import API
from src.fast_pipeline import Pipeline
from src.strategy.smc_strad import *
from src.strategy.rsi_strad import *


BASE_DIR = Path(__file__).resolve().parent
PARQUET_FILE = Path("C:/dev/QuantProjects/Cryptocurrencies/BTCUSD/btc_1min_clean.parquet")
DATA_DIR = PARQUET_FILE.parent


def run_pipeline():
    my_strategies = \
    [
        SMCStrategy(name="SMC_1to2", rrr=1.0),
        #RSIStrategy(name="SMC_1to3", rrr=3.0)
    ]

    pipe = Pipeline(
        data_path=PARQUET_FILE,
        start_date="2025-02-01T00:00:00",
        end_date="2025-12-01T23:59:59",
        start_time="14:30",
        end_time="16:30",
        session=None,
        strategies=my_strategies
    )
    pipe.run()


def download_market_candles():
    downloader = API(
        api_key=POLYGON_API_KEY,
        tickers=["X:BTCUSD"],
        frame="minute",
        parquet_file=PARQUET_FILE,
        start_fallback="2025-01-01"  # Nur falls die Datei noch gar nicht existiert
    )
    downloader.run()  # Die API kümmert sich jetzt selbst um den Rest!


######################################################


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