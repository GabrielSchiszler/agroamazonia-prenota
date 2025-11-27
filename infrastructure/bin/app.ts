#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AgroAmazoniaStack } from '../lib/agroamazonia-stack';

const app = new cdk.App();
new AgroAmazoniaStack(app, 'AgroAmazoniaStack', {
  env: { 
    account: process.env.CDK_DEFAULT_ACCOUNT, 
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});
