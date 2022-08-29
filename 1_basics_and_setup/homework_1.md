# Week 1 Homework

In this homework we'll prepare the environment and practice with terraform and SQL

## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have?

To get the version, run `gcloud --version`

<details>
    <summary>Answer</summary>

Google Cloud SDK 396.0.0

</details>

## Google Cloud account

Create an account in Google Cloud and create a project.

## Question 2. Terraform

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

- `terraform init`
- `terraform plan`
- `terraform apply`

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

<details>
    <summary>Answer</summary>

    ❯ terraform apply
    var.project
    Your GCP Project ID

    Enter a value: dtc-de-358703


    Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
    + create

    Terraform will perform the following actions:

    # google_bigquery_dataset.dataset will be created
    + resource "google_bigquery_dataset" "dataset" {
        + creation_time              = (known after apply)
        + dataset_id                 = "trips_data_all"
        + delete_contents_on_destroy = false
        + etag                       = (known after apply)
        + id                         = (known after apply)
        + last_modified_time         = (known after apply)
        + location                   = "us-west2"
        + project                    = "dtc-de-358703"
        + self_link                  = (known after apply)

        + access {
            + domain         = (known after apply)
            + group_by_email = (known after apply)
            + role           = (known after apply)
            + special_group  = (known after apply)
            + user_by_email  = (known after apply)

            + dataset {
                + target_types = (known after apply)

                + dataset {
                    + dataset_id = (known after apply)
                    + project_id = (known after apply)
                    }
                }

            + view {
                + dataset_id = (known after apply)
                + project_id = (known after apply)
                + table_id   = (known after apply)
                }
            }
        }

    # google_storage_bucket.data-lake-bucket will be created
    + resource "google_storage_bucket" "data-lake-bucket" {
        + force_destroy               = true
        + id                          = (known after apply)
        + location                    = "US-WEST2"
        + name                        = "dtc_data_lake_dtc-de-358703"
        + project                     = (known after apply)
        + self_link                   = (known after apply)
        + storage_class               = "STANDARD"
        + uniform_bucket_level_access = true
        + url                         = (known after apply)

        + lifecycle_rule {
            + action {
                + type = "Delete"
                }

            + condition {
                + age                   = 30
                + matches_prefix        = []
                + matches_storage_class = []
                + matches_suffix        = []
                + with_state            = (known after apply)
                }
            }

        + versioning {
            + enabled = true
            }
        }

    Plan: 2 to add, 0 to change, 0 to destroy.

    Do you want to perform these actions?
    Terraform will perform the actions described above.
    Only 'yes' will be accepted to approve.

    Enter a value: yes

    google_bigquery_dataset.dataset: Creating...
    google_storage_bucket.data-lake-bucket: Creating...
    google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/dtc-de-358703/datasets/trips_data_all]
    google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=dtc_data_lake_dtc-de-358703]

    Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

</details>

## Prepare Postgres

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

    ```bash
    wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
    ```

You will also need the dataset with zones:

    ```bash
    wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
    ```

Download this data and put it to Postgres

## Question 3. Count records

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

<details>
    <summary>Answer</summary>
53024 taxi trips

    SELECT
        COUNT(*) num_trips
    FROM yellow_taxi_data
    WHERE tpep_pickup_datetime::date = '2021-01-15';

</details>

## Question 4. Largest tip for each day

Find the largest tip for each day.
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

<details>
    <summary>Answer</summary>
2021-01-20, 1140.44

    SELECT
        tpep_pickup_datetime::date pickup_date,
        MAX(tip_amount) largest_tip
    FROM yellow_taxi_data
    GROUP BY tpep_pickup_datetime::date
    ORDER BY largest_tip DESC
    LIMIT 1;

</details>

## Question 5. Most popular destination

What was the most popular destination for passengers picked up
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

<details>
    <summary>Answer</summary>
Upper East Side South, 97 trips

    ```
    SELECT
        COALESCE(dozones."Zone", 'Unknown') as zone,
        COUNT(*) as num_trips
    FROM yellow_taxi_data as taxi

    INNER JOIN zones as puzones
    ON taxi."PULocationID" = puzones."LocationID"

    LEFT JOIN zones as dozones
    ON taxi."DOLocationID" = dozones."LocationID"

    WHERE
        puzones."Zone" ilike '%central park%'
        AND tpep_pickup_datetime::date = '2021-01-14'
    GROUP BY 1
    ORDER BY num_trips DESC
    LIMIT 1;
    ```

</details>

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

<details>
    <summary>Answer</summary>
Alphabet City/Unknwon, 2292.4

    ```
    SELECT
        CONCAT(
            COALESCE(puzones."Zone", 'Unknown'),
            '/',
            COALESCE(dozones."Zone", 'Unknown')) as pickup_dropoff_zone,
        AVG(total_amount) as avg_price
    FROM yellow_taxi_data as taxi
    LEFT JOIN zones as puzones
    ON taxi."PULocationID" = puzones."LocationID"
    LEFT JOIN zones as dozones
    ON taxi."DOLocationID" = dozones."LocationID"
    GROUP BY 1
    ORDER BY avg_price DESC
    LIMIT 1;
    ```

</details>
