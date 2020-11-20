# inbulk
Declarative ETL framework.

Perform ETL processing without loading any data in memory.

# Support
- BigQuery

# Usage
## Configure ETL Job
```user.yaml
init:
  credential-file: /.gcp/credential.json
  service: bigquery
in:
  query: |
    select *
    from Datalake.users
    where
      _partitiontime > '${last_modified}'
  vars:
    - name: last_modified
      database: DWH
      table: users
      mode: meta
      field: last_modified_time
      default: '2020-01-01'
out:
  project: project
  database: DWH
  table: users
  mode: merge
  merge:
    order:
      - column: modified
        desc: True
    keys:
      - id
```

## Execute on docker
```sh
docker run -v path-to-credential.json:/.gcp/credential.json -v $PWD:/src davincistd/inbulk:0.1.0 inbulk /src/user.yaml 
```

### dry-run
The query to be executed is displayed.
```sh
docker run -v path-to-credential.json:/.gcp/credential.json -v $PWD:/src davincistd/inbulk:0.1.0 inbulk /src/user.yaml --dry-run
```

# Configuration
|Field|Description|
|-|-|
|in.query| Query|
|in.vars| Settings for embedding existing table data and meta information in queries.<br>If you write it in a query like `${name}`, it will be expanded at runtime.<br>Can be used for difference execution, etc.|
|in.vars[].default|Set the value to be used if the table does not exist.|
|out.project|Destination GCP Project|
|out.database|Destination dataset name|
|out.table| Destination table name|
|out.mode| Mode of addition methods.<br> One of (append, replace, merge).|
|out.merge|Set only when mode is merge.|
|out.merge.order|List of fields to prioritize in case of duplication.|
|out.merge.order[].column|Column to prioritize in case of duplication(e.g. modified_at).|
|out.merge.order[].desc|Set the order in ascending or descending order so that they are in order of priority.<br>Default is False.|
|out.merge.keys|List of non-duplicate fields(e.g. id).|
