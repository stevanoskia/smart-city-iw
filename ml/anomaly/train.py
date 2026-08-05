import joblib
from pathlib import Path
from sklearn.ensemble import IsolationForest
from features import load_raw, build_features

MODEL_PATH = Path(__file__).parent / "model_anomaly.joblib"
CONTAMINATION = 0.05

def main():
    df, feature_cols = build_features(load_raw())
    df = df.dropna(subset=feature_cols)

    model = IsolationForest(n_estimators=200, contamination=CONTAMINATION, random_state=42)
    model.fit(df[feature_cols])

    flagged = (model.predict(df[feature_cols]) == -1).sum()
    print(f"Trained on {len(df)} rows, flagged {flagged} anomalies ({flagged/len(df):.1%})")

    joblib.dump({"model": model, "feature_cols": feature_cols}, MODEL_PATH)
    print(f"Saved -> {MODEL_PATH}")

if __name__ == "__main__":
    main()