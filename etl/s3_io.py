import boto3
import json

def write_json_to_s3(bucket : str, key : str, payload:dict ) -> str:
	s3 = boto3.client("s3")
	body = json.dumps.(payload, separators=(",",":"), ensure_ascii=False).encode("utf-8")
	s3.put_object(
		Bucket=bucket,
		Key=key,
		Body=body,
		ContentType="application/json"
	)

	return f"s3://{bucket}/{key}"
