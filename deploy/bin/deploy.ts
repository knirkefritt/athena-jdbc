#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { DeployLambdaStack } from '../lib/deploy-lambda-stack';

const app = new cdk.App();

new DeployLambdaStack(app, 'athena-lambda-poc', {
    env: {
        account: '123456789012',
        region: 'eu-west-1'
    }
});
