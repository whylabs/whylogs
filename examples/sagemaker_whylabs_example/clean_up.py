import argparse

import boto3
from utils import delete_endpoint, delete_endpoint_config, delete_model


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--profile", type=str, default="default", help="AWS Profile name")
    parser.add_argument("-e", "--endpoint_name", type=str, default="whylabs-sagemaker", help="SageMaker endpoint name")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    profile_name = args.profile
    endpoint_name = args.endpoint_name
    session = boto3.session.Session(profile_name=profile_name)
    region_name = session.region_name
    # Instantiate sagemaker client
    sg = session.client("sagemaker", region_name=region_name)
    # Delete sagemaker endpoint
    delete_endpoint(sg, endpoint_name)
    try:
        delete_endpoint_config(sg, f"{endpoint_name}-config")
        delete_model(sg, endpoint_name)
    except Exception as e:
        print(e.response)
