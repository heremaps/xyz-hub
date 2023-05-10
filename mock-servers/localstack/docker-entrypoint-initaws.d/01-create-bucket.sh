#!/bin/bash

awslocal s3api create-bucket --bucket test-bucket --create-bucket-configuration LocationConstraint=eu-west-1
