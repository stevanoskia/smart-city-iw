import joblib
from pathlib import Path
from datetime import timedelta
from features import load_raw, build_features, get_engine

HORIZON = 1
MODEL_PATH = Path(__file__).parent / f"model_comfort_{HORIZON}d.joblib"

def main():
    bundle = joblib.load(MODEL_PATH)
    model, feature_cols = bundle["model"], bundle["feature_cols"]

    df, _ = build_features(load_raw(), horizon_days=HORIZON)
    latest = df.sort_values("date_utc").groupby("city").tail(1).dropna(subset=feature_cols).copy()
    if latest.empty:
        print("No cities with complete features yet.")
        return

    latest["predicted_comfort"] = model.predict(latest[feature_cols])
    latest["predicted_for"] = latest["date_utc"] + timedelta(days=HORIZON)

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in latest.iterrows():
            conn.exec_driver_sql(
                """
                insert into ml_predictions.city_score_forecast
                    (city, predicted_for, horizon_days, predicted_comfort_index, model_version)
                values (%s, %s, %s, %s, %s)
                on conflict (city, predicted_for, horizon_days)
                do update set predicted_comfort_index = excluded.predicted_comfort_index,
                              scored_at = now()
                """,
                (row["city"], row["predicted_for"], HORIZON, float(row["predicted_comfort"]), "xgb_v1"),
            )
    print(f"Wrote {len(latest)} predictions.")

if __name__ == "__main__":
    main()