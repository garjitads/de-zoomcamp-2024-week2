# **DTC (Data Talks Club) Data Engineering Zoomcamp 2024**

# Week 2 : Workflow Orchestration

## LEARNING

###  Intro to Orchestration

[Intro to Orchestration.pptx](https://github.com/garjita63/de-zoomcamp-2024/files/14094284/Intro.to.Orchestration.pptx)


### Intro to Mage
- Mage platform introduction
- The fundamental concepts behind Mage
- Get started
- Spin Mage up via Docker 🐳
- Run a simple pipeline

[Intro to Mage.pptx](https://github.com/garjita63/de-zoomcamp-2024/files/14094325/Intro.to.Mage.pptx)


###  ETL: API to Postgres
- Build a simple ETL pipeline that loads data from an API into a Postgres database
- Database will be built using Docker
- It will be running locally, but it's the same as if it were running in the cloud.

_Resources_

Taxi Dataset https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

**Building pipeline**

**Pipeline Tree**
  
![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/1c62a297-eea5-4fef-8bfc-5e28816ebbdd)


**Blocks List**

_load_taxi_data_

  ![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/09e0f165-1beb-4943-9c9f-b92526feb0fd)

```
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
   
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'pasangger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RateCodeID': pd.Int64Dtype(),
                    'store_and_fwd+flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_mount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float
                }

    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    return pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```

_transform_taxi_data_

 ![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/29b597ec-2114-44ee-adf4-020be4903c9b)

 ```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())
    
    return data[data['passenger_count'] > 0]

@test
def test_output(output, *args):
    assert output ['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'

```

 _sql_taxi_data_

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/7a372683-e31a-4d67-b519-363e12792018)

```
SELECT * FROM ny_taxi.yellow_cab_data LIMIT 10
```
 

###  ETL: API to GCS

In this tutorial will walk through the process of using Mage to extract, transform, and load data from an API to Google Cloud Storage (GCS). Covering both writing partitioned and unpartitioned data to GCS and discuss why you might want to do one over the other. Many data teams start with extracting data from a source and writing it to a data lake before loading it to a structured data source, like a database.

**Building pipeline**

**Pipeline Tree**

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/a7e76d24-2020-416f-8318-ef764dc64240)

**Blok List**

*api_to_gcs*

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/6511f061-ee57-4461-a36b-f130ad9bd74e)

```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-zoomcamp-ketut-1'
    object_key = 'yellow_tripdata_2021-01.csv'

    return GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```


### ETL: GCS to BigQuery

Now that we've written data to GCS, let's load it into BigQuery. In this section, we'll walk through the process of using Mage to load our data from GCS to BigQuery. This closely mirrors a very common data engineering workflow: loading data from a data lake into a data warehouse.

**Building pipeline**

**Pileline Tree**

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/1ee38674-46c4-43bc-8633-373ff41436c4)

**Blocks List**

*extract_taxi_gcs*

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/1b084c7f-31c5-451a-a85c-171630252950)

```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'de-zoomcamp-garjita-bucket'
    object_key = 'yellow_tripdata_2023-11.parquet'

    return GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```

*transform_taxi_gcs*

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/672a906a-7760-49e3-b9d3-368c5258ebbf)

```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
    )
    
    return data
```

*load_to_bigquery*

![image](https://github.com/garjita63/de-zoomcamp-2024/assets/77673886/e664aafb-06ab-4ca9-b088-74bd1e33812b)

```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'dtc-de-course-2024-411803.taxidataset.yellow_tripdata_2023-11'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
```


### Parameterized Execution

*Resources*

- [Mage Variables Overview](https://docs.mage.ai/development/variables/overview)
- [Mage Runtime Variables](https://docs.mage.ai/getting-started/runtime-variable)


### Deployment (Optional)

Cover deploying Mage using Terraform and Google Cloud.

*Resources*

- [Installing Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
- [Installing gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [Mage Terraform Templates](https://github.com/mage-ai/mage-ai-terraform-templates)


### Additional Mage Guides

- [Terraform](https://docs.mage.ai/production/deploying-to-cloud/using-terraform)
- [Deploying to GCP with Terraform](https://docs.mage.ai/production/deploying-to-cloud/gcp/setup)


### 📑Additional Resources

- [Mage Docs](https://docs.mage.ai/introduction/overview)
- [Mage Guides](https://docs.mage.ai/guides/overview)
- [Mage Slack](https://mageai.slack.com/ssb/redirect)
 

[Notes from Jonah Oliver](https://www.jonahboliver.com/blog/de-zc-w2)


## Homework

[Questions - Week 2 Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/02-workflow-orchestration/homework.md)

