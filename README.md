# Overview

This repository demonstrates how to load a sample dataset into WarpStream for integration with [ClickHouse ClickPipes](https://clickhouse.com/cloud/clickpipes) product.

The sample dataset can be downloaded [here](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/logs-532.csv.gz).

Once it's downloaded, you can follow [these instructions](https://github.com/warpstreamlabs/warpstream-fly-io-template) to deploy a test WarpStream cluster to Fly.io or [these instructions](https://railway.app/template/30Xa3Y?referralCode=kKBYG0) to deploy on Railway.

Once you have a functioning WarpStream cluster with SASL credentials, you can run this script:

```
go run ./main.go -broker $BOOTSTRAP_URL:9092 -username $SASL_USERNAME -password $SASL_PASSWORD
```