# metrics.py
import json
import time
import boto3
import os

cloudwatch = boto3.client('cloudwatch')
NAMESPACE = os.environ.get("METRICS_NAMESPACE", "VideoPipeline")

def put_metric(name, value, unit="Milliseconds", dims=None):
    """Send one custom metric to CloudWatch."""
    dims = dims or {}
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            'MetricName': name,
            'Dimensions': [{'Name': k, 'Value': str(v)[:255]} for k, v in dims.items()],
            'Unit': unit,
            'Value': float(value)
        }]
    )

def log_event(message, **fields):
    """Structured JSON log line (great for CloudWatch Logs Insights)."""
    payload = {"msg": message, **fields}
    print(json.dumps(payload))

def size_bucket(bytes_):
    mb = bytes_ / (1024.0 * 1024.0)
    if mb < 20:    return "S<20MB"
    if mb < 200:   return "20-200MB"
    if mb < 2048:  return "200MB-2GB"
    return ">2GB"
