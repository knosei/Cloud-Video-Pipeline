import boto3
import subprocess
import os
import sys
import json
import time
from botocore.exceptions import ClientError

# Import metrics helpers from the Lambda layer
from metrics import put_metric, log_event

s3 = boto3.client('s3')

PROCESSED_BUCKET = 'video-output-processed-kno'  # single bucket for unprocessed/ processed/ final/

def file_size_mb(path):
    """Return file size in MB (rounded)."""
    return round(os.path.getsize(path) / (1024 * 1024), 2) if os.path.exists(path) else 0

def split_video(input_bucket, input_key):
    video_id = os.path.splitext(os.path.basename(input_key))[0]
    segment_prefix = f"unprocessed/{video_id}/"
    local_input = '/tmp/input.mp4'

    start = time.time()
    log_event("Split started", video_id=video_id, bucket=input_bucket)

    # Download
    s3.download_file(input_bucket, input_key, local_input)
    input_size = file_size_mb(local_input)
    put_metric("InputVideoSizeMB", input_size)

    # Decide segmentation strategy
    if input_size > 500:  # Large file, use bigger segments (5min)
        segment_time = "300"
    else:
        segment_time = "60"

    # Split
    segment_pattern = '/tmp/segment-%03d.mp4'
    subprocess.run([
        'ffmpeg', '-i', local_input,
        '-c', 'copy', '-map', '0',
        '-segment_time', segment_time, '-f', 'segment',
        segment_pattern
    ], check=True)

    # Upload segments
    i = 0
    segment_keys = []
    total_seg_size = 0
    while True:
        segment_path = f'/tmp/segment-{i:03d}.mp4'
        if not os.path.exists(segment_path):
            break
        segment_key = f"{segment_prefix}segment-{i:03d}.mp4"
        s3.upload_file(segment_path, PROCESSED_BUCKET, segment_key)
        segment_keys.append(segment_key)
        total_seg_size += file_size_mb(segment_path)
        i += 1

    # Metadata
    metadata = {
        "segments": segment_keys,
        "bucket": PROCESSED_BUCKET,
        "segment_metadata_key": f"{segment_prefix}segments.json",
        "video_id": video_id
    }
    with open("/tmp/segments.json", "w") as f:
        json.dump(metadata, f)
    s3.upload_file("/tmp/segments.json", PROCESSED_BUCKET, metadata["segment_metadata_key"])

    elapsed = time.time() - start
    put_metric("VideoSplits", 1)
    put_metric("SegmentsGenerated", len(segment_keys))
    put_metric("SplitDurationSec", elapsed)
    put_metric("SplitThroughputMBps", input_size / elapsed if elapsed > 0 else 0)
    put_metric("TotalSegmentSizeMB", total_seg_size)

    log_event("Split complete",
              video_id=video_id,
              segments=len(segment_keys),
              duration_sec=elapsed,
              input_size_mb=input_size,
              total_seg_size_mb=total_seg_size)


def process_segment(input_bucket, input_key):
    start = time.time()
    print(f"[PROCESS] Bucket={input_bucket} Key={input_key}")

    if not input_key.startswith("unprocessed/"):
        print("[PROCESS][WARN] Expected key under unprocessed/, got:", input_key)

    local_in = '/tmp/segment.mp4'
    local_out = '/tmp/segment_out.mp4'
    try:
        s3.download_file(input_bucket, input_key, local_in)
    except ClientError as e:
        log_event("Process download error", error=str(e), key=input_key)
        sys.exit(1)

    input_size = file_size_mb(local_in)
    output_key = input_key.replace("unprocessed/", "processed/", 1)

    # Transcode to H.264 + AAC
    subprocess.run([
        'ffmpeg', '-i', local_in,
        '-c:v', 'libx264', '-preset', 'fast', '-crf', '28',
        '-c:a', 'aac', '-b:a', '128k',
        local_out
    ], check=True)

    try:
        s3.upload_file(local_out, input_bucket, output_key)
    except ClientError as e:
        log_event("Process upload error", error=str(e), key=output_key)
        sys.exit(1)

    elapsed = time.time() - start
    put_metric("SegmentsProcessed", 1)
    put_metric("SegmentProcessDurationSec", elapsed)
    put_metric("SegmentSizeMB", file_size_mb(local_out))

    log_event("Segment processed",
              input=input_key,
              output=output_key,
              duration_sec=elapsed,
              size_mb=input_size)


def merge_segments():
    start = time.time()
    meta_key = os.environ.get('SEGMENT_METADATA_KEY') or os.environ.get('SEGMENT_JSON')
    if not meta_key:
        orig_key = os.environ['S3_KEY']
        video_id = os.path.splitext(os.path.basename(orig_key))[0]
        meta_key = f"unprocessed/{video_id}/segments.json"

    log_event("Merge started", metadata_key=meta_key)

    try:
        resp = s3.get_object(Bucket=PROCESSED_BUCKET, Key=meta_key)
    except ClientError as e:
        log_event("Merge metadata error", error=str(e), key=meta_key)
        sys.exit(1)

    data = json.loads(resp['Body'].read().decode('utf-8'))
    segment_keys = data["segments"]
    video_id = data.get("video_id") or os.path.basename(meta_key).split('/')[1]

    list_file_path = '/tmp/segments.txt'
    with open(list_file_path, 'w') as f:
        for i, seg_key in enumerate(segment_keys):
            processed_key = seg_key.replace("unprocessed/", "processed/", 1)
            local_seg = f"/tmp/seg_{i:03d}.mp4"
            try:
                s3.download_file(PROCESSED_BUCKET, processed_key, local_seg)
            except ClientError as e:
                log_event("Merge download error", error=str(e), key=processed_key)
                sys.exit(1)
            f.write(f"file '{local_seg}'\n")

    merged_path = "/tmp/merged.mp4"
    subprocess.run([
        'ffmpeg', '-f', 'concat', '-safe', '0', '-i', list_file_path,
        '-c:v', 'libx264', '-preset', 'fast', '-crf', '28',
        '-c:a', 'aac', '-b:a', '128k',
        merged_path
    ], check=True)

    output_size = file_size_mb(merged_path)
    final_key = f"final/{video_id}.mp4"
    s3.upload_file(merged_path, PROCESSED_BUCKET, final_key)

    elapsed = time.time() - start
    put_metric("VideosMerged", 1)
    put_metric("MergeDurationSec", elapsed)
    put_metric("FinalVideoSizeMB", output_size)

    log_event("Merge complete",
              video_id=video_id,
              final_key=final_key,
              duration_sec=elapsed,
              final_size_mb=output_size,
              compression_ratio=round(
                  output_size / max(1, sum(file_size_mb(f"/tmp/seg_{i:03d}.mp4") for i in range(len(segment_keys)))),
                  2
              ))



def main():
    mode = os.environ.get('MODE', 'SPLIT').upper()
    bucket = os.environ.get('S3_BUCKET')
    key = os.environ.get('S3_KEY')

    log_event("Processor main entry", mode=mode, bucket=bucket, key=key)

    if mode == 'SPLIT':
        if key and key.startswith("unprocessed/"):
            sys.exit(1)
        split_video(bucket, key)

    elif mode == 'PROCESS':
        if not (bucket and key):
            sys.exit(1)
        process_segment(bucket, key)

    elif mode == 'MERGE':
        merge_segments()

    else:
        print(f"[ERROR] Unknown MODE: {mode}")
        sys.exit(1)

    sys.exit(0)

if __name__ == '__main__':
    main()
