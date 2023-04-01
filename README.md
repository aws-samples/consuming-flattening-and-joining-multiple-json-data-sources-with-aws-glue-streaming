# Consuming, flattening and joining multiple JSON data sources with AWS Glue Streaming
This repo contains code that demonstrates how to leverage AWS Glue streaming capabilities to process unbounded datasets, which arrive in the form of nested JSON key-value structures, 
from multiple data producers. 

## Solution Overview
![Alt text](docs/images/nested_joins_diagrams_architecture_dynamodb_sink.png?raw=true "Title")

In this solution, we use synthetic data generated by a Python script. The Python script writes the data to the two Amazon Kinesis Data Streams. 
Each of these data streams have a Glue Data Catalog table with the nested JSON structure defined. 
Next, an AWS Glue Streaming job reads the records from each data stream and joins them via a [stream-stream join](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins). 
With the Glue Streaming job we use native PySpark functions to flatten the nested JSON and join the streaming DataFrames. 
We also use Watermarks to manage the late arriving data. Finally, we write the data to an Amazon DynamoDB sink.

## Deployment

### Deploy the CloudFormation Script

To get started, navigate to the AWS CloudFormation console. Create a CloudFormation Stack with the template in the [infra folder](https://github.com/aws-samples/consuming-flattening-and-joining-multiple-json-data-sources-with-aws-glue-streaming/tree/main/infra).

For more details on how to do this, please see the [CloudFormation documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html) describing how to create a stack in the console.

### Upload the streaming job to S3
Upload the [script](src/streaming_join_job.py) to your new s3 bucket s3://*YOUR-BUCKET-NAME*/code/.

### Start the Glue streaming job.
1. On the AWS Glue console, choose ETL Jobs in the navigation pane.
2. In the "Your Jobs" section and choose the "awsglue-streaming-join-nested-json". 
3. In the top right corner, choose the "Run" button.


### Configure the Python virtual environment
This project is set up like a standard Python project. 
The initialization process also creates a virtualenv within this project, 
stored under the .venv directory. 
To create the virtualenv it assumes that there is a python3 (or python for Windows) 
executable in your path with access to the venv package. 
If for any reason the automatic creation of the virtualenv fails, you can create the virtualenv manually.
 
```
$ python3 -m venv .venv
```

The next step is to activate your virtualenv.

```
$ source .venv/bin/activate
```
If you are a Windows platform, you would activate the virtualenv like this:
```
% .venv\Scripts\activate.bat
```
Once the virtualenv is activated, you can install the required dependencies.

```
(.venv) $ pip install -r requirements.txt
```

### Run the data generation script
The solution comes with a Python script to generate the two data sources. 
Make sure you have [set up the boto3 credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) correctly. Then in the terminal, navigate to the data-gen folder of the code repo and run the script like below:


### Clean Up
#### Stop the Streaming Job
Streaming jobs will run continuously until you stop them. Navigate back to the Glue job in the console, click on the "Runs" Tab.
In the recent job runs, the latest job running will have a "Stop job run" button.

#### Infrastructure Clean Up
To avoid incurring future charges, delete the resources. You can do this by [deleting the CloudFormation stack](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-delete-stack.html) in the console.

```
# Script for generating the data
python3 data_generator.py --purchase_stream_name nestedPurchaseStream --recommender_stream_name recommenderStream --region ap-southeast-2
```

## License
This library is licensed under the MIT-0 License. See the LICENSE file.
