from data_loader import (
    get_fx_pairs_data,
    format_fx_pairs,
    filter_session,
    compute_log_returns,
)
from views import plot_price, plot_returns

START = "2025-01-01"
END = "2025-01-31"

def main():
    fx = get_fx_pairs_data(["C:EURUSD", "C:MXNZAR"], START, END)
    fx = format_fx_pairs(fx)

    # Session Filter
    eur_london = filter_session(fx["C:EURUSD"], session="london")
    mxn_london = filter_session(fx["C:MXNZAR"], session="london")

    # Price plots
    plot_price(eur_london, "C:EURUSD")
    plot_price(mxn_london, "C:MXNZAR")

    # Returns
    returns = {
        "C:EURUSD": compute_log_returns(eur_london),
        "C:MXNZAR": compute_log_returns(mxn_london),
    }

    plot_returns(returns, START, END)


if __name__ == "__main__":
    main()
