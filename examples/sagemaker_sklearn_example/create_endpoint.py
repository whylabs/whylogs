import boto3
from dotenv import dotenv_values

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
    client = session.client('sagemaker', region_name=region_name)
    endpoints = client.list_endpoints()
    endpoint_name_list = [(ep["EndpointName"], ep["EndpointStatus"]) for ep in endpoints["Endpoints"]]
    for check_name in endpoint_name_list:
        if endpoint_name == check_name[0]:
            return check_name[1]
    return None

def deploy_endpoint(
        profile: str, region: str, image_uri: str, environment: dict,
        endpoint_name: str, instance_type: str, role: str
    ) -> None:
    """
    Deploy a SageMaker endpoint. 

    Args:
        profile (str): AWS profile name.
        region (str): AWS region to deploy SageMaker endpoint.
        image_uri (str): Amazon ECR image URI.
        environment (dict): Environment variables dictionary,
        endpoint_name (str): Name of the SageMaker endpoint.
        instance_type (str): [description]
        role (str): [description]

    Raises:
        e: [description]
    """

    if is_endpoint_running(endpoint_name, profile, region) is not None:
        print("Endpoint already exist and will return.")
        return

    try:
        session = boto3.session.Session(profile_name=profile)
        sm = session.client('sagemaker', region_name=region)
        primary_container = {
            'Image': image_uri, 
            "Environment": environment,
        }        
        # Create sagemaker model
        _ = sm.create_model(
            ModelName=endpoint_name,
            ExecutionRoleArn=role,
            PrimaryContainer=primary_container,
        )

        # create endpoint configuration
        endpoint_config_name = endpoint_name + '-config'
        _ = sm.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    'InstanceType': instance_type,
                    'InitialVariantWeight': 1,
                    'InitialInstanceCount': 1,
                    'ModelName': endpoint_name,
                    'VariantName': 'AllTraffic'
                }
            ]
        )

        # create endpoint
        _ = sm.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )

    except Exception as e:
        print("Cannot create endpoint - Exception is >> {}".format(e))
        if type(e).__name__ == "StateMachineAlreadyExists":
            print("Skip creation because it was created before.")
        else:
            raise e
    print(f"Completed model endpoint {endpoint_name} deployment !!!")

if __name__ == "__main__":
    profile = "mfa"
    image_name = "whylabs-sagemaker"
    endpoint_name = "whylogs-sagemaker-v3"
    instance_type = "ml.m4.xlarge"
    environment = dotenv_values("code/.env")

    session = boto3.session.Session(profile_name=profile)
    sts = session.client("sts")
    # Get account ID and region of current profile
    account_id = sts.get_caller_identity().get("Account")
    region = session.region_name
    image_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{image_name}:latest"
    # SageMaker execution role
    role = f"arn:aws:iam::{account_id}:role/SageMakerExecution"

    deploy_endpoint(profile, region, image_uri, environment, endpoint_name, instance_type, role)
