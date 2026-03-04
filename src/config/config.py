import yaml
from pathlib import Path

# Basis-Path dynamic
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# load YAML
yaml_path = PROJECT_ROOT / "config.yaml"
with open(yaml_path, 'r') as f:
    _data = yaml.safe_load(f)

PARQUET_FILE = PROJECT_ROOT / _data['paths']['data_relative']

# Vars
START_DATE = _data['backtest']['start_date']
END_DATE = _data['backtest']['end_date']
START_TIME = _data['backtest']['start_time']
END_TIME = _data['backtest']['end_time']
SESSION = _data['backtest']['session']
BALANCE = _data['backtest']['balance']
RISK_PERCENT = _data['backtest']['risk_percent']

ACTIVE_STRATEGIES = _data['active_strategies']

DOWNLOAD_TICKERS = _data['download']['tickers']
DOWNLOAD_FRAME = _data['download']['frame']
DOWNLOAD_START_FALLBACK = _data['download']['start_fallback']