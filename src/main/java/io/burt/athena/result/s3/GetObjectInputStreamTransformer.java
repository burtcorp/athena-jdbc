package io.burt.athena.result.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GetObjectInputStreamTransformer extends InputStreamResponseTransformer {
    private final S3AsyncClient s3Client;
    private final GetObjectRequest.Builder requestBuilder;
    private Duration timeout;
    private int bytesOffset = 0;

    public GetObjectInputStreamTransformer(S3AsyncClient s3Client, GetObjectRequest.Builder requestBuilder, Duration timeout) {
        this.s3Client = s3Client;
        this.requestBuilder = requestBuilder;
        this.timeout = timeout;
    }

    @Override
    public void onResponse(GetObjectResponse r) {
        requestBuilder.ifMatch(r.eTag());
        super.onResponse(r);
    }

    @Override
    protected boolean ensureChunk() throws IOException {
        try {
            return super.ensureChunk();
        } catch (IOException cause) {
            Throwable originalError = error;
            try {
                error = null;
                readChunk = null;
                chunks.clear();
                requestBuilder.range("bytes=" + bytesOffset + "-");
                s3Client.getObject(requestBuilder.build(), this)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return super.ensureChunk();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cause.addSuppressed(e);
                error = originalError;
                throw cause;
            } catch (ExecutionException | IOException | TimeoutException e) {
                cause.addSuppressed(e);
                error = originalError;
                throw cause;
            }
        }
    }

    @Override
    public int read(byte[] destination, int offset, int length) throws IOException {
        int bytesRead = super.read(destination, offset, length);
        if (bytesRead >= 0) {
            bytesOffset += bytesRead;
        }
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        int byteValue = super.read();
        if (byteValue >= 0) {
            bytesOffset++;
        }
        return byteValue;
    }
}
