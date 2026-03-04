import polars as pl


class SessionProcessor:
    # In Sekunden ab Mitternacht
    SESSIONS = {
        "sy": (75600, 21600),   # 21:00 - 06:00 (Über Mitternacht!)
        "as": (82800, 28800),   # 23:00 - 08:00 (Über Mitternacht!)
        "lo": (25200, 57600),   # 07:00 - 16:00
        "ny": (43200, 75600)    # 12:00 - 21:00
    }

    @staticmethod
    def extrema(df: pl.DataFrame | pl.LazyFrame, names: list) -> pl.LazyFrame:
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df

        # Wir definieren die Join-Spalte einmal vorab
        lf = lf.with_columns(pl.col("t").dt.date().alias("join_date"))

        for name in names:
            if name not in SessionProcessor.SESSIONS:
                continue

            start, end = SessionProcessor.SESSIONS[name]

            if start > end:
                cond = (pl.col("t_int") >= start) | (pl.col("t_int") <= end)
            else:
                cond = (pl.col("t_int") >= start) & (pl.col("t_int") <= end)

            # Hier ist der Fix:
            # 1. Wir gruppieren nach 'join_date'
            # 2. Wir stellen sicher, dass das Ergebnis keine Spalte 't' mehr hat
            stats = (
                lf.filter(cond)
                .group_by("join_date")
                .agg([
                    pl.col("h").max().alias(f"{name}_high"),
                    pl.col("l").min().alias(f"{name}_low")
                ])
            )

            # Join über 'join_date' -> danach brauchen wir 'join_date' vom rechten DF nicht mehr
            lf = lf.join(stats, on="join_date", how="left")

        # Am Ende können wir die temporäre Hilfsspalte wieder löschen
        return lf.drop("join_date")