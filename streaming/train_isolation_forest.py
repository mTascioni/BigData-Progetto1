#!/usr/bin/env python3
import argparse
import json
import os

from isolation_forest_model import save_model, train_model

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-dir", default="/workspace/config")
    parser.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "models", "isolation_forest.pkl"))
    args = parser.parse_args()

    with open(os.path.join(args.config_dir, "experiment.json")) as f:
        experiment = json.load(f)

    model = train_model(experiment["health_channels_nominal"])
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    save_model(model, args.out)
    print(f"Modello salvato in {args.out}")

if __name__ == "__main__":
    main()
