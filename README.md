## Local Run Instructions

### Install Dependencies

pip install -r requirements.txt

### Run the Batch Job

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

### This will:

- Process data.csv
- Generate rolling mean and signal
- Write metrics.json
- Write run.log
- Print the final metrics JSON to stdout

## Docker Build and Run

### Build Docker Image

docker build -t mlops-task .

### Run Docker Container

docker run --rm mlops-task

The container will:

- Execute the batch job
- Generate metrics.json and run.log
- Print the final metrics JSON to stdout
- Exit with code 0 on success

## Example metrics.json

{
"version": "v1",
"rows_processed": 10000,
"metric": "signal_rate",
"value": 0.4989,
"latency_ms": 25,
"seed": 42,
"status": "success"
}