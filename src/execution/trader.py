import numpy as np
from numba import njit

# just in time simulator
@njit(fastmath=True)
def jit_simulator(o, h, l, c,
                  long_entries, short_entries,
                  entry_long, entry_short,
                  sl_long, sl_short,
                  tp_long, tp_short,
                  balance, risk_pc, max_parallel=5):
    n = len(c)
    pnl_curve = np.zeros(n)
    curr_balance = balance

    # Gebührensatz (0.05% pro Trade-Öffnung)
    fee_rate = 0.0005

    # Matrix: [Status (0=frei, 1=long, -1=short), Entry, SL, TP, Amount]
    active_trades = np.zeros((max_parallel, 5))

    for i in range(n):
        # --- A. EXIT MANAGEMENT ---
        for t in range(max_parallel):
            status = active_trades[t, 0]
            if status == 0: continue

            entry_p = active_trades[t, 1]
            sl_p = active_trades[t, 2]
            tp_p = active_trades[t, 3]
            amount = active_trades[t, 4]

            # LONG EXIT
            if status == 1:
                if l[i] <= sl_p:  # Stop Loss
                    curr_balance -= (entry_p - sl_p) * amount
                    active_trades[t, :] = 0
                elif h[i] >= tp_p:  # Take Profit
                    curr_balance += (tp_p - entry_p) * amount
                    active_trades[t, :] = 0

            # SHORT EXIT
            elif status == -1:
                if h[i] >= sl_p:  # Stop Loss
                    curr_balance -= (sl_p - entry_p) * amount
                    active_trades[t, :] = 0
                elif l[i] <= tp_p:  # Take Profit
                    curr_balance += (entry_p - tp_p) * amount
                    active_trades[t, :] = 0

        # --- B. ENTRY MANAGEMENT ---
        # Long Entry Check
        if long_entries[i] and curr_balance > 0:
            for t in range(max_parallel):
                if active_trades[t, 0] == 0:
                    e_p, s_p, t_p = entry_long[i], sl_long[i], tp_long[i]
                    risk_per_unit = e_p - s_p

                    # Check: Risiko muss positiv und signifikant sein (> 0.01% vom Preis)
                    if risk_per_unit > (e_p * 0.0001):
                        # 1. Theoretische Positionsgröße basierend auf Risiko
                        pos_size = (curr_balance * risk_pc) / risk_per_unit

                        # 2. Sicherheits-Cap: Maximaler Hebel (10x des aktuellen Kapitals)
                        max_pos = (curr_balance * 10.0) / e_p
                        final_amount = min(pos_size, max_pos)

                        # 3. Gebühr abziehen
                        curr_balance -= (e_p * final_amount * fee_rate)

                        active_trades[t, 0] = 1
                        active_trades[t, 1] = e_p
                        active_trades[t, 2] = s_p
                        active_trades[t, 3] = t_p
                        active_trades[t, 4] = final_amount
                    break

        # Short Entry Check
        elif short_entries[i] and curr_balance > 0:
            for t in range(max_parallel):
                if active_trades[t, 0] == 0:
                    e_p, s_p, t_p = entry_short[i], sl_short[i], tp_short[i]
                    risk_per_unit = s_p - e_p

                    if risk_per_unit > (e_p * 0.0001):
                        pos_size = (curr_balance * risk_pc) / risk_per_unit

                        max_pos = (curr_balance * 10.0) / e_p
                        final_amount = min(pos_size, max_pos)

                        curr_balance -= (e_p * final_amount * fee_rate)

                        active_trades[t, 0] = -1
                        active_trades[t, 1] = e_p
                        active_trades[t, 2] = s_p
                        active_trades[t, 3] = t_p
                        active_trades[t, 4] = final_amount
                    break

        pnl_curve[i] = curr_balance

    return pnl_curve