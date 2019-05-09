package io.burt.athena.support;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class GetObjectHelper implements S3Client {
    private final Map<String, String> objects;
    private final List<GetObjectRequest> getObjectRequests;

    public GetObjectHelper() {
        this.objects = new HashMap<>();
        this.getObjectRequests = new LinkedList<>();
    }

    public void setObject(String bucket, String key, String contents) {
        objects.put(String.format("s3://%s/%s", bucket, key), contents);
    }

    public List<GetObjectRequest> getObjectRequests() {
        return getObjectRequests;
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(Consumer<GetObjectRequest.Builder> getObjectRequestConsumer) throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder();
        getObjectRequestConsumer.accept(requestBuilder);
        GetObjectRequest request = requestBuilder.build();
        getObjectRequests.add(request);
        String uri = String.format("s3://%s/%s", request.bucket(), request.key());
        if (objects.containsKey(uri)) {
            String contents = objects.get(uri);
            GetObjectResponse response = GetObjectResponse.builder().build();
            AbortableInputStream stream = AbortableInputStream.create(new ByteArrayInputStream(contents.getBytes(Charset.forName("UTF-8"))));
            return new ResponseInputStream<>(response, stream);
        } else {
            throw NoSuchKeyException.builder().build();
        }
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
    }
}
