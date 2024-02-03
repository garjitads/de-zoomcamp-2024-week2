## Week 2: Workflow Orchestration - Data Engineering Zoomcamp 2024 ##

*Hands-On Learning*

***Source:***

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration

### Intro to Orchestration

[Intro to Orchestration.pptx](https://docs.google.com/presentation/d/17zSxG5Z-tidmgY-9l7Al1cPmz4Slh4VPK6o2sryFYvw/edit?pli=1#slide=id.p)

### Intro to Mage

- Mage platform introduction
  
- The fundamental concepts behind Mage
  
- Get started
  
- Spin Mage up via Docker 🐳
  
- Run a simple pipeline
  

[Intro to Mage.pptx](https://docs.google.com/presentation/d/1y_5p3sxr6Xh1RqE6N8o2280gUzAdiic2hPhYUUD6l88/)

Mage and Postgres on Docker Desktop

![](https://media.licdn.com/dms/image/D5612AQEw-1v4rKbuvg/article-inline_image-shrink_400_744/0/1706930454716?e=1712188800&v=beta&t=8Ny9lFUyPOMbd8y6gxzIGrnBOELDJGg-GlrGd0qJmpQ)

### ETL: API to Postgres

- Build a simple ETL pipeline that loads data from an API into a Postgres database
  
- Database will be built using Docker
  
- It will be running locally, but it's the same as if it were running in the cloud.
  

***Resources***

Taxi Dataset https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

- **Building Pipeline**

**Pipeline Tree**

![](https://media.licdn.com/dms/image/D5612AQFrsA7J6QusnA/article-inline_image-shrink_400_744/0/1706930642107?e=1712188800&v=beta&t=0xrixKep42qR0b9yboeIsmYqIcOGlbfVgpc5nnxAgso)

**Blocks List**

*load_taxi_data*

![](https://media.licdn.com/dms/image/D5612AQHl6Bbiy-Qidw/article-inline_image-shrink_400_744/0/1706930770732?e=1712188800&v=beta&t=l5Twjiae3Pa1y8kP_AWtSIZQLvSUPnKZ8RsKFDLDHuE)

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

*transform_taxi_data*

![](https://media.licdn.com/dms/image/D5612AQEqjmT4CzrNmw/article-inline_image-shrink_400_744/0/1706931081033?e=1712188800&v=beta&t=ele3RsiFrl130fel4B-aiE8kpYmDnKYVo3lPYTkBQ5I)

```
if 'transformer' not in globals():

    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():

    from mage_ai.data_preparation.decorators import test

@transformer

def transform(data, args, *kwargs):

    print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())

    return data[data['passenger_count'] > 0]

@test

def test_output(output, *args):

    assert output ['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
```

*taxi_data_to_pg*

![](https://media.licdn.com/dms/image/D5612AQFu0rgn0guyaQ/article-inline_image-shrink_400_744/0/1706931229198?e=1712188800&v=beta&t=XmEjHFYiLdrXxH41OrgsRQIkuz1E4wJshiu1t6CEfHw)

```
from mage_ai.settings.repo import get_repo_path

from mage_ai.io.config import ConfigFileLoader

from mage_ai.io.postgres import Postgres

from pandas import DataFrame

from os import path

if 'data_exporter' not in globals():

    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter

def export_data_to_postgres(df: DataFrame, **kwargs) -> None:

    schema_name = 'ny_taxi'  # Specify the name of the schema to export data to

    table_name = 'yellow_cab_data'  # Specify the name of the table to export data to

    config_path = path.join(get_repo_path(), 'io_config.yaml')

    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        loader.export(

            df,

            schema_name,

            table_name,

            index=False,  # Specifies whether to include index in exported table

            if_exists='replace',  # Specify resolution policy if table name already exists

        )
```

*sql_taxi_data*

![](https://media.licdn.com/dms/image/D5612AQFyfzRDBA_QoQ/article-inline_image-shrink_400_744/0/1706931542039?e=1712188800&v=beta&t=7iFQNN4XC9uosn0jJNk5Bh4Loizh6nf3UGek12dBeKU)

```
SELECT count(*) FROM ny_taxi.yellow_cab_data;
```

- API to Postgres Pipeline Execution

![](https://media.licdn.com/dms/image/D5612AQEzr3JSzkJ0JA/article-inline_image-shrink_1000_1488/0/1706931824790?e=1712188800&v=beta&t=cWuJz1c2HEMbT4Ub3N9DOwoo88lccoCTZCadsFqbdNc)

![](https://media.licdn.com/dms/image/D5612AQGACI90CzUNVg/article-inline_image-shrink_1000_1488/0/1706931877742?e=1712188800&v=beta&t=t6WBpJMIoYT4Om0XLOSa6MDIPEZfDlwfw5D58VLNayU)

![](https://media.licdn.com/dms/image/D5612AQFlQv_UX5TV8Q/article-inline_image-shrink_400_744/0/1706931924597?e=1712188800&v=beta&t=hYu2gs2-Iup0Esn3kfX5RkMtruuB6HmNud_mzU-ahKU)

### ETL: API to GCS

In this tutorial will walk through the process of using Mage to extract, transform, and load data from an API to Google Cloud Storage (GCS). Covering both writing partitioned and unpartitioned data to GCS and discuss why you might want to do one over the other. Many data teams start with extracting data from a source and writing it to a data lake before loading it to a structured data source, like a database.

- **Building Pipeline**

**Pipeline Tree**

![](https://media.licdn.com/dms/image/D5612AQGA2ISnArPh1w/article-inline_image-shrink_400_744/0/1706932121951?e=1712188800&v=beta&t=nJ_vdyJc4XQXEZh1IwvlHZc8VZUIOUezGoyklWBEkxQ)

**Blok List**

*api_to_gcs*

![](https://media.licdn.com/dms/image/D5612AQHdd-Rl8fxfsg/article-inline_image-shrink_400_744/0/1706932222653?e=1712188800&v=beta&t=eQEc4JSWRrkIjmS3tgksPH4k-h9PRIh63lHjumm-HTM)

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

- **API to GCS Pipeline Execution**

![](https://media.licdn.com/dms/image/D5612AQEz220WTDCf1g/article-inline_image-shrink_400_744/0/1706932331336?e=1712188800&v=beta&t=-SfLcQ2OvqaRnNgjy6Uo6qbjGo1E8EQ3dDs1uzPtBVE)

### ETL: GCS to BigQuery

Now that we've written data to GCS, let's load it into BigQuery. In this section, we'll walk through the process of using Mage to load our data from GCS to BigQuery. This closely mirrors a very common data engineering workflow: loading data from a data lake into a data warehouse.

Here I use another source file format (.parquet) that has been uploaded manually to GCS bucket.

[yellow_tripdata_2023-11.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet)

- **Building pipeline**

**Pipeline Tree**

![](https://media.licdn.com/dms/image/D5612AQGwIW_RIzh22g/article-inline_image-shrink_400_744/0/1706932516954?e=1712188800&v=beta&t=HbmWeVK93GVi5Djw3UO2ja_cBArruFvQytokaVakss4)

**Blocks List**

*extract_taxi_gcs*

![](https://media.licdn.com/dms/image/D5612AQFwlssU4sPQwA/article-inline_image-shrink_400_744/0/1706932570108?e=1712188800&v=beta&t=T9mv6k-U1-RVz8ysTo6EBOVXZNUenW5G5vwmcr4x-Ls)

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

load_to_bigquery

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

- **GCS to BigQuery Pipeline Execution**

![](https://media.licdn.com/dms/image/D5612AQEDxl6owAAjmg/article-inline_image-shrink_400_744/0/1706932667539?e=1712188800&v=beta&t=gEFeYCywIgoM5XC29pbFRPliSRFyOL6mLGVjhAp0yQs)

![](https://media.licdn.com/dms/image/D5612AQGSxvAxLaNVXA/article-inline_image-shrink_400_744/0/1706932757243?e=1712188800&v=beta&t=8uYqB-411EExak4gnBzluxQUpd0yRhzt8Tt9Z48XJOw)


### Deploying to GCP with Terraform

After completing the installation and setup of Mage on GCP using Terraform, here are the results after running terraform apply
```
module.lb-http.google_compute_backend_service.default["default"]: Creation complete after 59s [id=projects/dtc-de-course-2024-411803/global/backendServices/mage-data-gar-urlmap-backend-default]
module.lb-http.google_compute_url_map.default[0]: Creating...
module.lb-http.google_compute_url_map.default[0]: Still creating... [10s elapsed]
module.lb-http.google_compute_url_map.default[0]: Creation complete after 12s [id=projects/dtc-de-course-2024-411803/global/urlMaps/mage-data-gar-urlmap-url-map]
module.lb-http.google_compute_target_http_proxy.default[0]: Creating...
module.lb-http.google_compute_target_http_proxy.default[0]: Still creating... [10s elapsed]
module.lb-http.google_compute_target_http_proxy.default[0]: Creation complete after 12s [id=projects/dtc-de-course-2024-411803/global/targetHttpProxies/mage-data-gar-urlmap-http-proxy]
module.lb-http.google_compute_global_forwarding_rule.http[0]: Creating...
module.lb-http.google_compute_global_forwarding_rule.http[0]: Still creating... [10s elapsed]
module.lb-http.google_compute_global_forwarding_rule.http[0]: Still creating... [20s elapsed]
module.lb-http.google_compute_global_forwarding_rule.http[0]: Creation complete after 24s [id=projects/dtc-de-course-2024-411803/global/forwardingRules/mage-data-gar-urlmap]

Apply complete! Resources: 7 added, 0 changed, 1 destroyed.

Outputs:

service_ip = "34.49.166.36"
```
$ terraform destroy


### Homework

[Questions - Week 2 Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/02-workflow-orchestration/homework.md)

![image](https://github.com/garjitads/de-zoomcamp-2024-week2/assets/157445647/8eb7b62d-adf2-4d35-b5cf-6f8f36aca657)

