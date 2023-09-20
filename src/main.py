import datetime as dt
import os

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI
from starlette.requests import Request

load_dotenv()

region = os.environ.get("S3_REGION")
bucket_name = os.environ.get("S3_BUCKET_NAME")
endpoint_url = os.environ.get("S3_ENDPOINT_URL")
access_key_id = os.environ.get("S3_ACCESS_KEY_ID")
secret_key = os.environ.get("S3_SECRET_KEY")
slug = os.environ.get("SLUG")

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post(f"/{slug}")
async def post(req: Request):
    """Receive a gzipped log and upload it to a s3 bucket"""
    session = boto3.Session(region_name=region)

    resource = session.resource(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
    )
    body = await req.body()

    fn = f"datadog-{dt.datetime.now().timestamp()}.zip"

    s3object = resource.Object(bucket_name, fn)

    s3object.put(Body=body, ACL="public-read")

    return {"result": "ok"}
