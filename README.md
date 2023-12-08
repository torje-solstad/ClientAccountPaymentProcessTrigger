# ClientAccountPaymentProcessrigger

## Description

A lambda trigger for initializing clientaccountpayementprocess

## Table of Contents

- [Installation Windows](#installation windows)

## Installation Windows

Install as normal golang project and build


```bash
# Download git
git clone https://github.com/your-username/your-project.git
cd your-project

# Build and zip

SET GOOS=linux
go build .\src\main


# In order to use as an AWS lambda, download build-lambda-zip.exe

go install github.com/aws/aws-lambda-go/cmd/build-lambda-zip@latest

# Zip the lambda function

%USERPROFILE%\Go\bin\build-lambda-zip.exe -o exampleFunction.zip main

# Upload as zip file to aws. Remember to reference your zip file name when configuring the lambda in AWS. AWS uses hello as default (in this example zipfile is called exampleFunction.zip, so use exampleFunction)
