# Recharge Lambda Connector

This README outlines the steps to update an existing AWS Lambda function by zipping and uploading a new version of the Node.js code.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Zipping the Function](#zipping-the-function)
3. [Uploading to AWS Lambda](#uploading-to-aws-lambda)

---

## Prerequisites

1. Ensure AWS CLI is installed and configured:
    ```bash
    aws --version
    aws configure
    ```

If not installed, follow the guide [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
---

## Zipping the Function

1. Navigate to the directory containing your Node.js code.
    ```bash
    cd /path/to/your/lambda-function
    ```
  
2. Zip the contents of the folder, including dependencies.
    ```bash
    zip -r my-lambda-function.zip .
    ```

---

## Uploading to AWS Lambda

1. Use the AWS CLI to update the Lambda function with the new zip file. Replace `your-function-name` with the name of your Lambda function.
    ```bash
    aws lambda update-function-code \
    --function-name your-function-name \
    --zip-file fileb://my-lambda-function.zip
    ```

---

Now, your AWS Lambda function should be updated with the new code. You can go to the AWS Lambda console to perform any tests or check logs for further verification.