import json
import argparse
import boto3

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--profile', type=str, default="default",
        help='AWS Profile name')
    parser.add_argument(
        '-e', '--endpoint_name', type=str, default="whylabs-sagemaker",
        help='SageMaker endpoint name')
    parser.add_argument(
        '-i', '--instance', type=str, default="ml.m4.xlarge",
        help="SageMaker instance type to serve your endpoint.")
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    profile_name = args.profile
    endpoint_name = args.endpoint_name
    session = boto3.session.Session(profile_name=profile_name)
    region_name = session.region_name
    # Payload for /invocations endpoint
    r = json.dumps({
        "sepal_length_cm": 5.1,
        "sepal_width_cm": 3.5,
        "petal_length_cm": 1.4,
        "petal_width_cm": 0.2
    })
    # Invoke the endpoint using
    sg = session.client("runtime.sagemaker", region_name=region_name)
    response = sg.invoke_endpoint(
        EndpointName=endpoint_name,
        Body=r,
        ContentType='application/json',
    )
    # Decode the response
    print(json.loads(response["Body"].read().decode("utf-8")))