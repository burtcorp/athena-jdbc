package io.burt.athena.result;

import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.time.Duration;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class PreloadingStandardResultTest extends StandardResultTest {
    protected StandardResult createResult(AthenaAsyncClient athenaClient) {
        QueryExecution queryExecution = QueryExecution.builder().queryExecutionId("Q1234").build();
        return new PreloadingStandardResult(athenaClient, queryExecution, 123, Duration.ofMillis(10));
    }
}
