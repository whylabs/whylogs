# Amazon SageMaker Scikit-learn example (Doc in progress)

SageMaker custom container with scikit-learn inference example that integrates whylabs logging.

## Requirements

- Docker
- An AWS account and AWS CLI installed
- Python 3
- Conda (or any dependency and environment management for Python. e.g. venv.)
- An AWS user that have permissions for SageMakerFullAccess and pushing docker image to ECR repository.

## Create a conda environment

If you have conda installed, use the included environment.yml file to create a conda environment with these requirements using the following command:

```bash
conda env create -f environment.yml
conda activate whylabs
```

Otherwise, to be able to download the dataset, train the model, deploy the SageMaker endpoint and test it from your computer, the following dependencies are required:

```bash
boto3==1.18.39
kaggle==1.5.12
python-dotenv==0.19.0
scikit-learn==0.24.2
pandas==1.3.2
```

## Download data and train the model

This example uses the iris-dataset. For downloading and training the model run the following commands:

```bash
cd code/
chmod +x download_iris.sh
./download_iris.sh
```

To train the model:

```bash
python train.py
```

As a result, you will get a __model.joblib__.

## Configure .env file

Create a .env file inside the code directory. A .env.example file is included as a template, you can adapt it according to your requirements. This .env file will be ignored by docker build but loaded in the next step as a dictionary of environment variables to the container that will be running on SageMaker host.

## Build docker image an push it to AWS ECR

SageMaker uses docker images to run your algorithm. In order to pass an image to SageMaker, you need to build it and push it to Amazon Elastic Container Registry (ECR). The following script will create and ECR repository, login to it, build your image, retag it accordingly and push it to the repository.

```bash
chmod +x build_push.sh
./build_push.sh whylabs-sagemaker <your-aws-profile-name>
```

__Note:__ You can find the aws profile names inside ~/.aws folder.

## Deploy endpoint to SageMaker

You should modify the variables values according to configuration made in the last step:

- profile
- image_name
- endpoint_name
- instance_type (in case you want to use another type of instance.)

```bash
python create_endpoint.py -p <your-aws-profile-name> -i <sagemaker-instance-type> -e <endpoint-name> 
```

## Test endpoint

Once your endpoint has been created, to test it run the following script.

```bash
python test_endpoint.py -p <your-aws-profile-name> -e <endpoint-name>
```

The json response printed should look like this:

```bash
{'data': {'class': 'Iris-setosa'}, 'message': 'Success'}
```

## IMPORTANT NOTE: Clean up AWS created resources

Be careful with keeping your endpoint running because this will generate AWS charges. To clean up your endpoint, you can go to the console and delete it manually or execute the following script:

```bash
python clean_up.py -p <your-aws-profile-name> -e <endpoint-name>
```
