import joblib
from pathlib import Path
from datetime import timedelta
from features import load_raw, build_features, get_engine

HORIZON = 24
MODEL_PATH = Path(__file__).parent / f"model_pm25_{HORIZON}h.joblib"

def main():
    bundle = joblib.load(MODEL_PATH)
    model, feature_cols = bundle["model"], bundle["feature_cols"]

    df, _ = build_features(load_raw(), horizon_hours=HORIZON)
    latest = df.sort_values("observed_at").groupby("city").tail(1).dropna(subset=feature_cols).copy()
    if latest.empty:
        print("No cities with complete features yet.")
        return

    latest["predicted_pm2_5"] = model.predict(latest[feature_cols])
    latest["predicted_for"]   = latest["observed_at"] + timedelta(hours=HORIZON)

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in latest.iterrows():
            conn.exec_driver_sql(
                """
                insert into ml_predictions.aqi_forecast
                    (city, predicted_for, horizon_hours, predicted_pm2_5, model_version)
                values (%s, %s, %s, %s, %s)
                on conflict (city, predicted_for, horizon_hours)
                do update set predicted_pm2_5 = excluded.predicted_pm2_5,
                              scored_at = now()
                """,
                (row["city"], row["predicted_for"], HORIZON, float(row["predicted_pm2_5"]), "xgb_v1"),
            )
    print(f"Wrote {len(latest)} predictions.")

if __name__ == "__main__":
    main()