#!/usr/bin/env python
import os
from google.cloud import storage

def upload_spark_jobs(bucket_name=None, project=None):
    """
    Uploads all files from project_root/spark_jobs to the GCS spark_jobs folder.
    
    """

    bucket_name = bucket_name or os.getenv("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable must be set")
    
    project = project or os.getenv("PROJECT_ID")
    if not project:
        raise ValueError("PROJECT_ID environment variable must be set")

 
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)


    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    local_folder = os.path.join(project_root, "spark_jobs")

    all_files = [os.path.join(root, f) for root, _, files in os.walk(local_folder) for f in files]

  
    for idx, path in enumerate(all_files, 1):
        relative_path = os.path.relpath(path, local_folder).replace("\\", "/")
        blob = bucket.blob(f"spark_jobs/{relative_path}")
        blob.upload_from_filename(path)
        print(f"[{idx}/{len(all_files)}] Uploaded {relative_path}")

    print(f"\n✅ Total {len(all_files)} files uploaded to gs://{bucket_name}/spark_jobs/")

if __name__ == "__main__":
    upload_spark_jobs()