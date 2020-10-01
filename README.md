# Azure Data Explorer Ingestion using Java SDK

Azure Data Explorer is a fast and highly scalable data exploration service for log and telemetry data. It provides a [Java client library](kusto/api/java/kusto-java-client-library.md) for interacting with the Azure Data Explorer service that can be to ingest, issue control commands and query data in Azure Data Explorer clusters.

In this example, you first create a table and data mapping in a test cluster. You then queue an ingestion to the cluster using the Java SDK and validate the results.

## Prerequisites

* If you don't have an Azure subscription, create a [free Azure account](https://azure.microsoft.com/free/) before you begin.
* Install [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).
* Install JDK (version 1.8 or later)
* Install [Maven](https://maven.apache.org/download.cgi)
* Create an [Azure Data Explorer cluster and database](create-cluster-database-portal.md).
* Create an [App Registration and grant it permissions to the database](provision-azure-ad-app.md) - save the client ID and client secret to be used later in the tutorial

## Run the application

When you run the sample code, the following actions are performed:

- **Drop table**: `StormEvents` table is dropped (if it exists).
- **Table creation**: `StormEvents` table is created.
- **Mapping creation**: `StormEvents_CSV_Mapping` mapping is created.
- **File ingestion**: A CSV file (in Azure Blob Storage) is queued for ingestion.

The sample code as seen in this snippet from `App.java`: 

```java
public static void main(final String[] args) throws Exception {
    dropTable(database);
    createTable(database);
    createMapping(database);
    ingestFile(database);
}
```

> To try different combinations of operations, you can uncomment/comment the respective methods in `App.java`.

1. Clone the sample code from GitHub:

    ```console
    git clone https://github.com/Azure-Samples/azure-data-explorer-java-sdk-ingest.git
    cd azure-data-explorer-java-sdk-ingest
    ```

2. Set the service principal information with the cluster endpoint and the database name in the form of environment variables that will be used by the program:

    ```console
    export AZURE_SP_TENANT_ID="<replace with tenantID>"
    export AZURE_SP_CLIENT_ID="<replace with appID>"
    export AZURE_SP_CLIENT_SECRET="<replace with password>"
    export KUSTO_ENDPOINT="https://<cluster name>.<azure region>.kusto.windows.net"
    export KUSTO_DB="name of the database"
    ```

3. Build and run:

    ```console
    mvn clean package
    java -jar target/adx-java-ingest-jar-with-dependencies.jar
    ```

    You'll get a similar output:

    ```console
    Table dropped
    Table created
    Mapping created
    Waiting for ingestion to complete...
    ```
    
    It might take a few minutes for the ingestion process to complete and you should see a log message `Ingestion completed successfully` once that happens. You can exit the program at this point and move to the next step. It will not impact the ingestion process, since it has already been queued.

## Validate

Wait for 5 to 10 minutes for the queued ingestion to schedule the ingestion process and load the data into Azure Data Explorer. 

Sign in to [https://dataexplorer.azure.com](https://dataexplorer.azure.com) and connect to your cluster. Then run the following command to get the count of records in the `StormEvents` table.

    ```kusto
    StormEvents | count
    ```

## Troubleshoot

1. Run the following command in your database to see if there were any ingestion failures in the last four hours. Replace the database name before running.

    ```kusto
    .show ingestion failures
    | where FailedOn > ago(4h) and Database == "<DatabaseName>"
    ```

2. Run the following command to view the status of all ingestion operations in the last four hours. Replace the database name before running.

    ```kusto
    .show operations
    | where StartedOn > ago(4h) and Database == "<DatabaseName>" and Operation == "DataIngestPull"
    | summarize arg_max(LastUpdatedOn, *) by OperationId
    ```

## Clean up resources

If you plan to follow our other articles, keep the resources you created. If not, run the following command in your database to drop the `StormEvents` table.

```kusto
.drop table StormEvents
```

## Resources

- [What is Azure Data Explorer?](https://docs.microsoft.com/en-us/azure/data-explorer/data-explorer-overview)
- [Azure Data Explorer data ingestion overview](https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview)
- [Azure Data Explorer Java SDK](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/java/kusto-java-client-library)