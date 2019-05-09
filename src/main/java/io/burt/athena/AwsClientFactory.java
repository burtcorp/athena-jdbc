package io.burt.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.AthenaAsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

class AwsClientFactory {
    AthenaAsyncClient createAthenaClient(Region region) {
        AthenaAsyncClientBuilder builder = AthenaAsyncClient.builder();
        builder.region(region);
        return builder.build();
    }

    S3Client createS3Client(Region region) {
        S3ClientBuilder builder = S3Client.builder();
        builder.region(region);
        return builder.build();
    }
}
