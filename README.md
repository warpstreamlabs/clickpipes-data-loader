# Overview

This repository demonstrates how to load a sample dataset into WarpStream for integration with [ClickHouse ClickPipes](https://clickhouse.com/cloud/clickpipes) product.

Sample datasets of various sizes are available (rows, uncompressed size, compressed size) covering a period of 30 days:

- [66m,20GB,1.9GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-66.csv.gz)
- [133m,38GB,4.2GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-133.csv.gz)
- [267m,76GB,8.2GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-267.csv.gz)
- [534m,152GB,16.1GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-534.csv.gz)
- [1064m,304GB,31.6GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-1064.csv.gz)
- [52804m,304GB,260.7GB](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/data-5280.csv.gz)

An ordered sample dataset can be downloaded [here](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/logs-534.csv.gz) containing 534m rows.

Once it's downloaded, you can follow [these instructions](https://github.com/warpstreamlabs/warpstream-fly-io-template) to deploy a test WarpStream cluster to Fly.io or [these instructions](https://railway.app/template/30Xa3Y?referralCode=kKBYG0) to deploy on Railway.

Once you have a functioning WarpStream cluster with SASL credentials, you can run this script:

```
go run ./main.go -broker $BOOTSTRAP_URL:9092 -username $SASL_USERNAME -password $SASL_PASSWORD
```
