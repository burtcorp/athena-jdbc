package io.burt.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.AthenaAsyncClientBuilder;

public class AwsClientFactory {
    public AthenaAsyncClient createAthenaClient(Region region) {
        AthenaAsyncClientBuilder builder = AthenaAsyncClient.builder();
        builder.region(region);
        return builder.build();
    }
}
