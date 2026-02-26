import s3fs

fs = s3fs.S3FileSystem(
    key="admin",
    secret="password",
    client_kwargs={
        "endpoint_url": "http://minio:9000"
    },
    config_kwargs={
        "s3": {"addressing_style": "path"}
    }
)

print(fs.ls("bronze"))