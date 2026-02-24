import numpy as np
from numba import njit


@njit
def jit_simulator(o, h, l, c,
                  long_entries, short_entries,
                  entry_prices, sl_prices, tp_prices,
                  balance, risk_pc, max_parallel=5):
    n = len(c)
    pnl_curve = np.zeros(n)
    curr_balance = balance

    # Trade-Matrix: [Status (0=frei, 1=long, -1=short), Entry, SL, TP, Amount]
    # Shape: (max_parallel, 5)
    active_trades = np.zeros((max_parallel, 5))

    for i in range(n):
        # --- A. EXIT MANAGEMENT ---
        for t in range(max_parallel):
            if active_trades[t, 0] == 0: continue  # Slot ist leer

            # LONG EXIT
            if active_trades[t, 0] == 1:
                if l[i] <= active_trades[t, 2]:  # SL getroffen
                    curr_balance -= (active_trades[t, 1] - active_trades[t, 2]) * active_trades[t, 4]
                    active_trades[t, :] = 0
                elif h[i] >= active_trades[t, 3]:  # TP getroffen
                    curr_balance += (active_trades[t, 3] - active_trades[t, 1]) * active_trades[t, 4]
                    active_trades[t, :] = 0

            # SHORT EXIT
            elif active_trades[t, 0] == -1:
                if h[i] >= active_trades[t, 2]:  # SL getroffen
                    curr_balance -= (active_trades[t, 2] - active_trades[t, 1]) * active_trades[t, 4]
                    active_trades[t, :] = 0
                elif l[i] <= active_trades[t, 3]:  # TP getroffen
                    curr_balance += (active_trades[t, 1] - active_trades[t, 3]) * active_trades[t, 4]
                    active_trades[t, :] = 0

        # --- B. ENTRY MANAGEMENT ---
        # Prüfen auf Long Signal
        if long_entries[i]:
            for t in range(max_parallel):
                if active_trades[t, 0] == 0:  # Finde freien Slot
                    e_p, s_p, t_p = entry_prices[i], sl_prices[i], tp_prices[i]
                    risk = e_p - s_p
                    if risk > 0:
                        active_trades[t, 0] = 1
                        active_trades[t, 1] = e_p
                        active_trades[t, 2] = s_p
                        active_trades[t, 3] = t_p
                        active_trades[t, 4] = (curr_balance * risk_pc) / risk
                    break  # Nur einen Trade pro Signal öffnen

        # Prüfen auf Short Signal
        elif short_entries[i]:
            for t in range(max_parallel):
                if active_trades[t, 0] == 0:
                    e_p, s_p, t_p = entry_prices[i], sl_prices[i], tp_prices[i]
                    risk = s_p - e_p
                    if risk > 0:
                        active_trades[t, 0] = -1
                        active_trades[t, 1] = e_p
                        active_trades[t, 2] = s_p
                        active_trades[t, 3] = t_p
                        active_trades[t, 4] = (curr_balance * risk_pc) / risk
                    break

        pnl_curve[i] = curr_balance
    return pnl_curve