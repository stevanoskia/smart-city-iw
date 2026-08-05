import joblib
from pathlib import Path
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from features import load_raw, build_features

HORIZON = 1
MODEL_PATH = Path(__file__).parent / f"model_traffic_{HORIZON}h.joblib"

def main():
    df, feature_cols = build_features(load_raw(), horizon_hours=HORIZON)
    df = df.dropna(subset=feature_cols + ["target_congestion"])

    train, test = train_test_split(df, test_size=0.25, random_state=42)
    print(f"train={len(train)}  test={len(test)}")

    model = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)
    model.fit(train[feature_cols], train["target_congestion"])

    mae = mean_absolute_error(test["target_congestion"], model.predict(test[feature_cols]))
    baseline_mae = mean_absolute_error(test["target_congestion"], test["congestion_score"])
    print(f"Model MAE ({HORIZON}h): {mae:.4f}")
    print(f"Naive baseline MAE (predict = current value): {baseline_mae:.4f}")

    joblib.dump({"model": model, "feature_cols": feature_cols}, MODEL_PATH)
    print(f"Saved -> {MODEL_PATH}")

if __name__ == "__main__":
    main()