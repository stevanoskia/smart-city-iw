import joblib
from pathlib import Path
from features import load_raw, build_features, get_engine

MODEL_PATH = Path(__file__).parent / "model_anomaly.joblib"

def main():
    bundle = joblib.load(MODEL_PATH)
    model, feature_cols = bundle["model"], bundle["feature_cols"]

    df, _ = build_features(load_raw())
    latest = df.sort_values("observed_at").groupby("city").tail(1).dropna(subset=feature_cols).copy()
    if latest.empty:
        print("No cities with complete features yet.")
        return

    latest["anomaly_score"] = model.decision_function(latest[feature_cols])
    latest["is_anomaly"] = model.predict(latest[feature_cols]) == -1

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in latest.iterrows():
            conn.exec_driver_sql(
                """
                insert into ml_predictions.pollution_anomaly
                    (city, observed_at, pm2_5_ug_m3, anomaly_score, is_anomaly, model_version)
                values (%s, %s, %s, %s, %s, %s)
                on conflict (city, observed_at)
                do update set anomaly_score = excluded.anomaly_score,
                              is_anomaly = excluded.is_anomaly,
                              scored_at = now()
                """,
                (row["city"], row["observed_at"], float(row["pm2_5_ug_m3"]),
                 float(row["anomaly_score"]), bool(row["is_anomaly"]), "iforest_v1"),
            )
    print(f"Wrote {len(latest)} rows ({latest['is_anomaly'].sum()} flagged as anomalies).")

if __name__ == "__main__":
    main()