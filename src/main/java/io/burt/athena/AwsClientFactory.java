package io.burt.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;

public class AwsClientFactory {
    public AthenaClient createAthenaClient(Region region) {
        AthenaClientBuilder builder = AthenaClient.builder();
        builder.region(region);
        return builder.build();
    }
}
