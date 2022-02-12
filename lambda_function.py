import json
import csv
import boto3
from botocore.exceptions import ClientError
import uuid

AWS_BUCKET = "csvaas"


def convertJSONToCSV(json_str):
    jdata = json.loads(json_str)
    file_uuid = str(uuid.uuid4())
    filename = "/tmp/data" + file_uuid + ".csv"

    cfile = open(filename, "w", newline="\n")
    csv_writer = csv.writer(
        cfile, delimiter=";", quotechar=",", quoting=csv.QUOTE_MINIMAL
    )

    is_header = 0

    for data in jdata:
        if is_header == 0:
            header = data.keys()
            csv_writer.writerow(header)
            is_header = 1

        csv_writer.writerow(data.values())

    cfile.close()

    client = boto3.client("s3")
    try:
        nfilename = file_uuid + ".csv"
        response = client.upload_file(filename, AWS_BUCKET, nfilename)
    except ClientError as e:
        print(str(e))
        return False

    # Return data
    response = client.get_object(Bucket=AWS_BUCKET, Key=nfilename)
    file = response["Body"].read().decode("utf-8")

    # Delete Data in S3
    client.delete_object(Bucket=AWS_BUCKET, Key=nfilename)
    return file


def lambda_handler(event, context):
    file = convertJSONToCSV(event["JSON"])

    return {
        "headers": {"Content-Type": "text/csv"},
        "statusCode": 200,
        "body": file,
        "isBase64Encoded": True,
    }
