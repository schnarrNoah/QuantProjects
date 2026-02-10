from config.config import TICKERS, START, END, SESSION
from pipeline import build_returns_pipeline
from views import plot_returns

def main():
    returns = build_returns_pipeline(
        TICKERS, START, END, SESSION
    )
    plot_returns(returns, START, END)

if __name__ == "__main__":
    main()
