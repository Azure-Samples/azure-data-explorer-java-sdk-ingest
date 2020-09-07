package com.azure.dataexplorer.sample.ingest;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT;
import com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportLevel;
import com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;

/**
 * Example to demonstrate Data Explorer ingestion and management operations
 */
public class App {
    static String clientIDEnvVar = "AZURE_SP_CLIENT_ID";
    static String clientSecretEnvVar = "AZURE_SP_CLIENT_SECRET";
    static String endpointEnvVar = "KUSTO_ENDPOINT";
    static String databaseEnvVar = "KUSTO_DB";

    static String clientID = null;
    static String clientSecret = null;
    static String endpoint = null;
    static String database = null;

    public static void main(final String[] args) throws Exception {
        dropTable(database);
        createTable(database);
        createMapping(database);
        ingestFile(database);
    }

    /**
     * validates whether required environment variables are present
     */
    static {
        clientID = System.getenv(clientIDEnvVar);
        if (clientID == null) {
            throw new IllegalArgumentException("Missing environment variable " + clientIDEnvVar);
        }

        clientSecret = System.getenv(clientSecretEnvVar);
        if (clientSecret == null) {
            throw new IllegalArgumentException("Missing environment variable " + clientSecretEnvVar);
        }

        endpoint = System.getenv(endpointEnvVar);
        if (endpoint == null) {
            throw new IllegalArgumentException("Missing environment variable " + endpointEnvVar);
        }

        database = System.getenv(databaseEnvVar);
        if (database == null) {
            throw new IllegalArgumentException("Missing environment variable " + databaseEnvVar);
        }

    }

    /**
     * creates a Client object
     * 
     * @return Client object to execute control and query operations
     * @throws Exception
     */
    static Client getClient() throws Exception {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(endpoint, clientID,
                clientSecret);

        return ClientFactory.createClient(csb);
    }

    // ifexists can also be added e.g. .drop table StormEvents ifexists
    static String dropTableCommand = ".drop table StormEvents";

    /**
     * drops a table from the database
     * 
     * @param database name of the kusto database
     */
    static void dropTable(String database) {
        try {
            getClient().execute(database, dropTableCommand);
        } catch (Exception e) {
            System.out.println("Failed to drop table: " + e.getMessage());
        }
        System.out.println("Table dropped");
    }

    static final String createTableCommand = ".create table StormEvents (StartTime: datetime, EndTime: datetime, EpisodeId: int, EventId: int, State: string, EventType: string, InjuriesDirect: int, InjuriesIndirect: int, DeathsDirect: int, DeathsIndirect: int, DamageProperty: int, DamageCrops: int, Source: string, BeginLocation: string, EndLocation: string, BeginLat: real, BeginLon: real, EndLat: real, EndLon: real, EpisodeNarrative: string, EventNarrative: string, StormSummary: dynamic)";

    /**
     * creates table in a database. to validate, run .show table StormEvents
     * 
     * @param database name of the kusto database
     */
    static void createTable(String database) {
        try {
            getClient().execute(database, createTableCommand);
        } catch (Exception e) {
            System.out.println("Failed to create table: " + e.getMessage());
            return;
        }
        System.out.println("Table created");
    }

    static final String createMappingCommand = ".create table StormEvents ingestion csv mapping 'StormEvents_CSV_Mapping' '[{\"Name\":\"StartTime\",\"datatype\":\"datetime\",\"Ordinal\":0}, {\"Name\":\"EndTime\",\"datatype\":\"datetime\",\"Ordinal\":1},{\"Name\":\"EpisodeId\",\"datatype\":\"int\",\"Ordinal\":2},{\"Name\":\"EventId\",\"datatype\":\"int\",\"Ordinal\":3},{\"Name\":\"State\",\"datatype\":\"string\",\"Ordinal\":4},{\"Name\":\"EventType\",\"datatype\":\"string\",\"Ordinal\":5},{\"Name\":\"InjuriesDirect\",\"datatype\":\"int\",\"Ordinal\":6},{\"Name\":\"InjuriesIndirect\",\"datatype\":\"int\",\"Ordinal\":7},{\"Name\":\"DeathsDirect\",\"datatype\":\"int\",\"Ordinal\":8},{\"Name\":\"DeathsIndirect\",\"datatype\":\"int\",\"Ordinal\":9},{\"Name\":\"DamageProperty\",\"datatype\":\"int\",\"Ordinal\":10},{\"Name\":\"DamageCrops\",\"datatype\":\"int\",\"Ordinal\":11},{\"Name\":\"Source\",\"datatype\":\"string\",\"Ordinal\":12},{\"Name\":\"BeginLocation\",\"datatype\":\"string\",\"Ordinal\":13},{\"Name\":\"EndLocation\",\"datatype\":\"string\",\"Ordinal\":14},{\"Name\":\"BeginLat\",\"datatype\":\"real\",\"Ordinal\":16},{\"Name\":\"BeginLon\",\"datatype\":\"real\",\"Ordinal\":17},{\"Name\":\"EndLat\",\"datatype\":\"real\",\"Ordinal\":18},{\"Name\":\"EndLon\",\"datatype\":\"real\",\"Ordinal\":19},{\"Name\":\"EpisodeNarrative\",\"datatype\":\"string\",\"Ordinal\":20},{\"Name\":\"EventNarrative\",\"datatype\":\"string\",\"Ordinal\":21},{\"Name\":\"StormSummary\",\"datatype\":\"dynamic\",\"Ordinal\":22}]'";

    /**
     * create a mapping reference. to validate, run .show table StormEvents
     * ingestion mappings
     * 
     * @param database
     */
    static void createMapping(String database) {
        try {
            getClient().execute(database, createMappingCommand);
        } catch (Exception e) {
            System.out.println("Failed to create mapping: " + e.getMessage());
            return;
        }
        System.out.println("Mapping created");
    }

    static IngestClient getIngestionClient() throws Exception {
        String ingestionEndpoint = "https://ingest-" + URI.create(endpoint).getHost();
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(ingestionEndpoint,
                clientID, clientSecret);

        return IngestClientFactory.createClient(csb);
    }

    static final String tableName = "StormEvents";
    static final String ingestionMappingRefName = "StormEvents_CSV_Mapping";
    static final String blobStorePathFormat = "https://%s.blob.core.windows.net/%s/%s%s";
    static final String blobStoreAccountName = "kustosamplefiles";
    static final String blobStoreContainer = "samplefiles";
    static final String blobStoreFileName = "StormEvents.csv";
    static final String blobStoreToken = "?st=2018-08-31T22%3A02%3A25Z&se=2020-09-01T22%3A02%3A00Z&sp=r&sv=2018-03-28&sr=b&sig=LQIbomcKI8Ooz425hWtjeq6d61uEaq21UVX7YrM61N4%3D";

    /**
     * queues ingestion to Azure Data Explorer and waits for it to complete or fail
     * 
     * @param database name of the kusto database
     * @throws InterruptedException
     */
    static void ingestFile(String database) throws InterruptedException {
        String blobPath = String.format(blobStorePathFormat, blobStoreAccountName, blobStoreContainer,
                blobStoreFileName, blobStoreToken);
        System.out.println(blobPath);
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath);

        IngestionProperties ingestionProperties = new IngestionProperties(database, tableName);
        ingestionProperties.setDataFormat(DATA_FORMAT.csv);
        ingestionProperties.setIngestionMapping(ingestionMappingRefName, IngestionMappingKind.Csv);
        ingestionProperties.setReportLevel(IngestionReportLevel.FailuresAndSuccesses);
        ingestionProperties.setReportMethod(IngestionReportMethod.QueueAndTable);

        CountDownLatch ingestionLatch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                IngestionResult result = null;
                try {
                    result = getIngestionClient().ingestFromBlob(blobSourceInfo, ingestionProperties);
                } catch (Exception e) {
                    System.out.println("Failed to initiate ingestion: " + e.getMessage());
                    ingestionLatch.countDown();
                }
                try {
                    IngestionStatus status = result.getIngestionStatusCollection().get(0);
                    while (status.status == OperationStatus.Pending) {
                        Thread.sleep(5000);
                        System.out.println("checking status>>>");
                        status = result.getIngestionStatusCollection().get(0);
                    }
                    System.out.println("Ingestion completed");
                    System.out.println("Final status: " + status.status);
                    ingestionLatch.countDown();
                } catch (Exception e) {
                    System.out.println("Failed to get ingestion status: " + e.getMessage());
                    ingestionLatch.countDown();
                }
            }

        }).start();

        System.out.println("Waiting for ingestion to complete...");
        ingestionLatch.await();
    }
}
