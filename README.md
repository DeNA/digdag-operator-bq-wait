# digdag-operator-bq-wait
Wait for a table or a partition in Google BigQuery.

# Usage
```
_export:
  plugin:
    repositories:
      - https://TBD/
    dependencies:
      - com.dena.digdag:digdag-operator-bq-wait:0.1.0

# Wait for a table to be created.
+step1:
  bq_wait>: some_dataset.some_table

# Wait for a table to be updated later than 1 hour after session time.
+step2:
  bq_wait>: some_dataset.some_table
  updated_after: PT1H

# Wait for a partition to be created.
+step3:
  bq_wait>: some_dataset.partitioned_table$${last_session_date_compact}
```
## tips
You can also activate plugin with parameters below defined in config file (server.properties).  
With these, you don't have to export plugin parameters in each dig file.
```
system-plugin.repositories = https://TBD/
system-plugin.dependencies = com.dena.digdag:digdag-operator-bq-wait:0.1.0
```

# Secrets (Optional)
When you want explicitly specify service account to access bq, use [gcp.credential](https://docs.digdag.io/operators/bq.html#secrets).  
If gcp.credential is not defined, bq_wait try to use [Application Default Credentials](https://cloud.google.com/docs/authentication/production).

# Options
- bq_wait>: (gcp_project).[dataset].[table]\($YYYYMMDD)
- updated_after: duration  
Wait for a table to be updated later than certain point of time.  
Duration must be in [ISO-8601 duration format](https://docs.oracle.com/javase/9/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).

Examples:  

Wait for a table to be updated later than 1 hour after session time.  
If the session time is 10 am, wait for a table to be newer than 11 am.
```
bq_wait>: some_dataset.some_table
updated_after: PT1H
```

Wait for a table to be updated later than 1 hour before session time.  
If the session time is 10 am, wait for a table to be newer than 9 am.
```
bq_wait>: some_dataset.some_table
updated_after: -PT1H
```

# Output parameters
- bq_wait.last_object
```
"table": {
    "project": "gcp-project",
    "dataset": "some_dataset",
    "table": "some_table"
},
"last_modified_time": 1581084640102
```

# Development
## static analysis
```
./gradlew spotbugsMain
```

## formatting and adding license header
```
./gradlew spotlessApply
```

## running tests
There're some prerequitites to run tests.  
Check out test codes for detail.
```
./gradlew test
```

## publishing to local repository.
Maven repository will be created in build/repo directory.
```
./gradlew publishShadowPublicationToLocalRepository
```

# License
[Apache License 2.0](./LICENSE)