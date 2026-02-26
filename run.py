import argparse
import yaml
import pandas as pd
import numpy as np
import logging
import json
import time
import sys
import os


# ----------------------------
# CLI Arguments
# ----------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    return parser.parse_args()


# ----------------------------
# Logging Setup
# ----------------------------
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# ----------------------------
# Load & Validate Config
# ----------------------------
def load_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError("Config file not found")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError("Config file is empty")

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    if not isinstance(config["seed"], int):
        raise ValueError("Seed must be integer")

    if not isinstance(config["window"], int) or config["window"] <= 0:
        raise ValueError("Window must be positive integer")

    return config


# ----------------------------
# Load & Validate Dataset
# ----------------------------
def load_data(path):
    if not os.path.exists(path):
        raise FileNotFoundError("Input CSV not found")

    try:
        # Read raw lines manually
        with open(path, "r") as f:
            lines = f.readlines()

        if not lines:
            raise ValueError("CSV is empty")

        # Remove surrounding quotes and newline characters
        lines = [line.strip().strip('"') for line in lines]

        # Split header
        header = lines[0].split(",")

        # Split remaining rows
        data_rows = [row.split(",") for row in lines[1:]]

        # Create dataframe manually
        df = pd.DataFrame(data_rows, columns=header)

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower()

        # Convert close column to numeric
        if "close" in df.columns:
            df["close"] = pd.to_numeric(df["close"], errors="coerce")

    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df
# ----------------------------
# Processing Logic
# ----------------------------
def process(df, window):
    logging.info("Computing rolling mean")

    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    logging.info("Generating signal")

    df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)

    return df


# ----------------------------
# Compute Metrics
# ----------------------------
def compute_metrics(df, version, seed, latency_ms):
    signal_rate = df["signal"].mean()

    return {
        "version": version,
        "rows_processed": len(df),
        "metric": "signal_rate",
        "value": round(float(signal_rate), 4),
        "latency_ms": int(latency_ms),
        "seed": seed,
        "status": "success"
    }


# ----------------------------
# Error Metrics Writer
# ----------------------------
def write_error_metrics(output_path, version, error_message):
    metrics = {
        "version": version if version else "unknown",
        "status": "error",
        "error_message": str(error_message)
    }

    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=4)

    print(json.dumps(metrics, indent=4))


# ----------------------------
# Main
# ----------------------------
def main():
    args = parse_args()
    setup_logging(args.log_file)

    start_time = time.time()

    try:
        logging.info("Job started")

        # Load config
        config = load_config(args.config)
        logging.info(f"Config loaded: {config}")

        # Set deterministic seed
        np.random.seed(config["seed"])

        # 🔥 DATASET IS LOADED HERE
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # Process
        df = process(df, config["window"])

        # Timing
        latency_ms = (time.time() - start_time) * 1000

        # Metrics
        metrics = compute_metrics(
            df,
            config["version"],
            config["seed"],
            latency_ms
        )

        # Write metrics file
        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=4)

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        # Print final metrics to stdout (Docker requirement)
        print(json.dumps(metrics, indent=4))

        sys.exit(0)

    except Exception as e:
        logging.exception("Job failed")

        version = None
        if "config" in locals() and "version" in config:
            version = config["version"]

        write_error_metrics(args.output, version, str(e))

        sys.exit(1)


if __name__ == "__main__":
    main()