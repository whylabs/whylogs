#!/bin/bash
# set -v
set -e

# This script shows how to build the Docker image and push it to ECR to be ready for use
# by SageMaker.

# The argument to this script is the image name. This will be used as the image on the local
# machine and combined with the account and region to form the repository name for ECR.
# Also the AWS profile name. The multiple profiles can be found in ~/.aws
image=${1:-whylabs-sagemaker}
profile=${2:-default}

echo "Image name ${image}"
echo "Profile name ${profile}"


# Get the account number associated with the current IAM credentials
account=$(aws --profile ${profile} sts get-caller-identity --query Account --output text)
# Get the region defined in the current configuration
region=$(aws --profile ${profile} configure get region)
# Repository name
fullname="${account}.dkr.ecr.${region}.amazonaws.com/${image}:latest"
# If the repository doesn't exist in ECR, create it.
aws --profile ${profile} ecr describe-repositories --repository-names "${image}" --region ${region} || \
    aws --profile ${profile} ecr create-repository --repository-name "${image}" --region ${region}
# Get password and login to ECR and execute it directly
aws --profile ${profile} ecr get-login-password --region ${region} | \
    docker login --username AWS --password-stdin ${account}.dkr.ecr.${region}.amazonaws.com/${image}
# Build the docker image, tag with full name and then push it to ECR
docker build -t ${image} .
docker tag ${image} ${fullname}
docker push ${fullname}

