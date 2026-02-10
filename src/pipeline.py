from data_loader import *

def build_returns_pipeline(tickers, start, end, session):
    fx = get_data(tickers, start, end)
    ffx = format_data(fx)

    returns = {}
    for t in tickers:
        df_sess = filter_session(ffx[t], session=session)
        returns[t] = compute_log_returns(df_sess)

    return returns
