import json
import boto3

profile_name = "mfa"
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
    EndpointName='whylogs-sagemaker-v2',Body=r,ContentType='application/json'
)
# Decode the response
print(json.loads(response["Body"].read().decode("utf-8")))