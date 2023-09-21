import datetime as dt
import logging
import os
from typing import Annotated

import boto3
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.requests import Request

load_dotenv()

region = os.environ.get("S3_REGION")
bucket_name = os.environ.get("S3_BUCKET_NAME")
endpoint_url = os.environ.get("S3_ENDPOINT_URL")
access_key_id = os.environ.get("S3_ACCESS_KEY_ID")
secret_key = os.environ.get("S3_SECRET_KEY")
slug = os.environ.get("SLUG")
username = os.environ.get("BASIC_AUTH_USERNAME")
password = os.environ.get("BASIC_AUTH_PASSWORD")

logging.warning(region)
logging.warning(bucket_name)
logging.warning(bucket_name)
logging.warning(access_key_id)

app = FastAPI()

security = HTTPBasic()

resource = boto3.resource(
    "s3",
    region_name=region,
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_key,
)


@app.get("/")
async def root():
    return {"response": "OK"}


@app.post(f"/{slug}")
async def post(req: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    """Receive a gzipped log and upload it to a s3 bucket"""

    if credentials.username != username or credentials.password != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    body = await req.body()

    object_path = f"datadog-{dt.datetime.now().timestamp()}.zip"

    s3object = resource.Object(bucket_name, object_path)

    s3object.put(Body=body)

    return {"result": "ok"}
