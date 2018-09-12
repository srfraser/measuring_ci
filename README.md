## Measuring CI

The Mozilla Release Engineering team is producing some estimates of our Continous Integration costs.

This repository contains a work-in-progress for the scripts used to gather and process the data.



### Current Architecture

`pushlog_scanner.py` and its dependencies are turned into a `.zip` file for use in an AWS
[Lambda](https://aws.amazon.com/lambda/) function. This gives us a place to run the script periodically without worrying about
other infrastructure. The lambda function's role gives it the appropriate access to an S3
bucket for the results.

The S3 is connected to [Athena](https://aws.amazon.com/athena/), which allows use of SQL-style queries against it. [Re:dash](https://sql.telemetry.mozilla.org/)
has a read-only api key to allow it access, and from there we can make queries and dashboards.


### Updating the Lambda Function

1. Clone the repository
2. Ensure Python 3.6 is installed (AWS Lambda currently only supports 2.7 and 3.6)
3. Run `create_lambda_func_in_docker.sh`
   If running on Linux, `create_lambda_func.sh` will do. Some modules require compilation, and the target platform is Linux,
   so installing natively on Mac/Windows is not going to work.
4. Copy the resulting zip file to S3: `aws s3 cp measuring_ci.zip s3://mozilla-releng-metrics/measuring_ci.zip`
5. Visit the [Lambda function's config page](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/measuring_ci_parquet_update)
   (If needed: ARN - `arn:aws:lambda:us-east-1:314336048151:function:measuring_ci_parquet_update`)
6. Under 'Function code' choose a 'Code entry type' of 'Upload a file from Amazon S3' and paste the above s3 url into the box.
7. Ensure the Handler is set correctly if not using lambda_handler() in lambda_function.py
8. Under 'Basic Settings' ensure the Memory usage is at 512Mb and Timeout is at least 2 minutes.
9. Click 'Save' at the top of the page
10. Test the lambda function using the 'Test' button. The test event itself doesn't matter as we're not using its data.
    If a test event is not defined, the basic 'Hello world' template will do.

### Planned Updates

The direct querying of a parquet file in S3 is a short-term step in order to get data visibility. Longer
term we will want to use the generic data ingestion method provided by the data team.
