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
    args = parser.parse_args()
    return args

def delete_endpoint(client: boto3.client, endpoint_name: str) -> None:
    try:
        _ = client.delete_endpoint(
            EndpointName=endpoint_name,
        )
        print(f"Endpoint {endpoint_name} deleted.")
    except Exception as e:
        print(e.response)
        

if __name__ == '__main__':
    args = parse_args()
    profile_name = args.profile
    endpoint_name = args.endpoint_name
    session = boto3.session.Session(profile_name=profile_name)
    region_name = session.region_name
    # Instantiate sagemaker client
    sg = session.client('sagemaker', region_name=region_name)
    # Delete sagemaker endpoint
    delete_endpoint(sg, endpoint_name)