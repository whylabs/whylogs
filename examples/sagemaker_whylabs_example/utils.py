import boto3


def is_endpoint_running(endpoint_name: str, profile_name: str, region_name: str) -> None:
    """
    Content of check_name could be "InService" or other.
    if the named endpoint doesn't exist then return None.

    Args:
        endpoint_name (str): Name of the SageMaker endpoint.
        profile_name (str): AWS profile name.
        region_name (str): AWS region to deploy SageMaker endpoint.
    Returns:
        None
    """
    session = boto3.session.Session(profile_name=profile_name)
    client = session.client("sagemaker", region_name=region_name)
    endpoints = client.list_endpoints()
    endpoint_name_list = [(ep["EndpointName"], ep["EndpointStatus"]) for ep in endpoints["Endpoints"]]
    for check_name in endpoint_name_list:
        if endpoint_name == check_name[0]:
            return check_name[1]
    return None


def delete_endpoint(client: boto3.client, endpoint_name: str) -> None:
    try:
        _ = client.delete_endpoint(
            EndpointName=endpoint_name,
        )
        print(f"Endpoint {endpoint_name} deleted.")
    except Exception as e:
        raise e.response["Error"]


def delete_model(client: boto3.client, model_name: str) -> None:
    try:
        _ = client.delete_model(ModelName=model_name)
        print(f"Model {model_name} deleted.")
    except Exception as e:
        raise e.response["Error"]


def delete_endpoint_config(client: boto3.client, endpoint_config: str) -> None:
    try:
        _ = client.delete_endpoint_config(EndpointConfigName=endpoint_config)
        print(f"Endpoint configuration {endpoint_config} deleted.")
    except Exception as e:
        raise e.response["Error"]
