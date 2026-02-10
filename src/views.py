import matplotlib.pyplot as plt


def plot_price(df, pair):
    plt.figure(figsize=(14, 6))
    plt.plot(df.index, df["c"], label="Close")
    plt.title(pair)
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_returns(ret_dict, start, end):
    fig, axes = plt.subplots(1, len(ret_dict), figsize=(14, 5), sharey=False)

    fig.suptitle(f"Returns Series {start} - {end}")

    for ax, (pair, returns) in zip(axes, ret_dict.items()):
        ax.plot(returns.index, returns.values)
        ax.set_title(pair)
        ax.set_xlabel("Time")
        ax.set_ylabel("Log Returns")

    plt.tight_layout()
    plt.show()
