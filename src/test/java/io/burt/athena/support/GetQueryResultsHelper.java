package io.burt.athena.support;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class GetQueryResultsHelper implements AthenaAsyncClient {
    private final List<GetQueryResultsRequest> resultRequests;

    private List<ColumnInfo> columns;
    private List<Row> remainingRows;

    public GetQueryResultsHelper() {
        this.resultRequests = new LinkedList<>();
        this.columns = null;
        this.remainingRows = null;
    }

    public static ColumnInfo createColumn(String label, String type) {
        return ColumnInfo.builder().label(label).type(type).build();
    }

    public static Row createRow(String... values) {
        List<Datum> data = new ArrayList<>(values.length);
        for (String value : values) {
            data.add(Datum.builder().varCharValue(value).build());
        }
        return Row.builder().data(data).build();
    }

    public static Row createRowWithNull() {
        return createRow(new String[]{null});
    }

    public int requestCount() {
        return resultsRequests().size();
    }

    public List<GetQueryResultsRequest> resultsRequests() {
        return resultRequests;
    }

    public List<String> nextTokens() {
        return resultRequests.stream().map(GetQueryResultsRequest::nextToken).collect(Collectors.toList());
    }

    public List<Integer> pageSizes() {
        return resultRequests.stream().map(GetQueryResultsRequest::maxResults).collect(Collectors.toList());
    }

    public void update(List<ColumnInfo> columns, List<Row> dataRows) {
        List<Datum> firstRow = new ArrayList<>(columns.size());
        this.columns = columns;
        for (ColumnInfo column : columns) {
            firstRow.add(Datum.builder().varCharValue(column.label()).build());
        }
        this.remainingRows = new LinkedList<>();
        this.remainingRows.add(Row.builder().data(firstRow).build());
        this.remainingRows.addAll(dataRows);
    }

    @Override
    public CompletableFuture<GetQueryResultsResponse> getQueryResults(Consumer<GetQueryResultsRequest.Builder> requestBuilderConsumer) {
        GetQueryResultsRequest.Builder requestBuilder = GetQueryResultsRequest.builder();
        requestBuilderConsumer.accept(requestBuilder);
        GetQueryResultsRequest request = requestBuilder.build();
        resultRequests.add(request);
        GetQueryResultsResponse.Builder responseBuilder = GetQueryResultsResponse.builder();
        List<Row> page;
        int pageSize = request.maxResults();
        int pageNum = request.nextToken() == null ? 1 : Integer.valueOf(request.nextToken());
        List<Row> tempPage = remainingRows.subList(0, Math.min(pageSize, remainingRows.size()));
        page = new ArrayList<>(tempPage);
        tempPage.clear();
        if (!remainingRows.isEmpty()) {
            responseBuilder.nextToken(String.valueOf(pageNum + 1));
        }
        responseBuilder.resultSet(rsb -> rsb.rows(page).resultSetMetadata(rsmb -> rsmb.columnInfo(columns)));
        return CompletableFuture.completedFuture(responseBuilder.build());
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}
