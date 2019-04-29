package io.burt.athena.result;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;

import java.time.Duration;

@ExtendWith(MockitoExtension.class)
class PreloadingStandardResultTest extends StandardResultTest {
    protected StandardResult createResult(AthenaAsyncClient athenaClient) {
        return new PreloadingStandardResult(athenaClient, "Q1234", 123, Duration.ofMillis(10));
    }
}
