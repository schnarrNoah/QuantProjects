import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Imports aus deinem Projekt
import src.config.config as cfg
from src.utils.api import API
from src.core.fast_pipeline import Pipeline
import src.strategy.smc as smc_module


def run_pipeline():
    """Bereitet die Strategien vor und startet die Backtest-Pipeline."""
    settings = {
        "start_date": cfg.START_DATE,
        "end_date": cfg.END_DATE,
        "start_time": cfg.START_TIME,
        "end_time": cfg.END_TIME,
        "session": cfg.SESSION
    }

    # Strategien dynamisch instanziieren
    my_strategies = []
    for strat_conf in cfg.ACTIVE_STRATEGIES:
        strat_class = getattr(smc_module, strat_conf["class_name"])
        instance = strat_class(
            name=strat_conf["display_name"],
            **strat_conf["params"]
        )
        my_strategies.append(instance)

    # Pipeline mit Config-Pfad ausführen
    pipe = Pipeline(
        data_path=cfg.PARQUET_FILE,
        strategies=my_strategies,
        filter_settings=settings
    )
    pipe.run()


def download_market_candles():
    """Führt den Datendownload basierend auf der Config aus."""
    api_key = os.getenv("APIKEY_MASSIVE")
    if not api_key:
        print("Abbruch: Kein API-Key in der .env Datei gefunden.")
        return

    downloader = API(
        api_key=api_key,
        tickers=cfg.DOWNLOAD_TICKERS,
        frame=cfg.DOWNLOAD_FRAME,
        parquet_file=cfg.PARQUET_FILE,
        start_fallback=cfg.DOWNLOAD_START_FALLBACK
    )
    downloader.run()


def main():
    # Umgebung laden (Pfade & .env)
    load_dotenv(cfg.PROJECT_ROOT / ".env")

    # Ordner erstellen, falls sie fehlen (Logik gehört in die Main)
    cfg.PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)

    # CLI-Logik: Download oder Backtest?
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print(f"Status: Starte Download für {cfg.DOWNLOAD_TICKERS}...")
        download_market_candles()
    else:
        print(f"Status: Starte Backtest mit {len(cfg.ACTIVE_STRATEGIES)} Strategien...")
        run_pipeline()


if __name__ == "__main__":
    main()