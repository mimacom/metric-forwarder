package com.mimacom.metrics.elasticsearch.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by _domine3 on 23.06.2017.
 */
@Component
public class IndexManager {
    private static String INDEX_NAME;
    private static String INDEX_NAME_DATE_FORMAT;

    @Autowired
    public IndexManager(@Value("${metricpoller.index.name:microsvcmetrics}") String indexName,
                        @Value("${metricpoller.index.dateFormat:yyyy-MM-dd}") String indexNameDateFormat) {
        INDEX_NAME = indexName;
        INDEX_NAME_DATE_FORMAT = indexNameDateFormat;
    }

    public String getIndexName(String endpoint) {
        return String.format("%s-%s-%s", INDEX_NAME, cleanEndpointName(endpoint), getCurrentLocalDateTimeStamp());
    }

    private String cleanEndpointName(String endpoint) {
        String result = endpoint.startsWith("/") ? endpoint.replaceFirst("/", "") : endpoint;
        return result.replace("/", "-");
    }

    private String getCurrentLocalDateTimeStamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern(INDEX_NAME_DATE_FORMAT));
    }
}
