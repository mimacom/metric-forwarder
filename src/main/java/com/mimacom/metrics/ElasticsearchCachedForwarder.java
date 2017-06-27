package com.mimacom.metrics;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author Enrique Llerena Dominguez
 */
@Component
public class ElasticsearchCachedForwarder {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchCachedForwarder.class);
    private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSSZ";

    private static final String POSFIX = ".value";
    private static final String META_KEY_TIMESTAMP = "timestamp" + POSFIX;
    private static final String META_KEY_HOST = "host" + POSFIX;
    private static final String META_KEY_PORT = "port" + POSFIX;
    private static final String META_KEY_SVC_ID = "serviceId" + POSFIX;
    private static final String BULK_ACTION_AND_METADATA = "{ \"index\" : { \"_type\" : \"%s\"} }";
    private static final String BULK_ENDPOINT = "_bulk";
    private static String INDEX_NAME;
    private static String INDEX_NAME_DATE_FORMAT;

    private final static Gson gson = new Gson();
    private final RestClient esRestClient;
    private final Map<String, String> mappings;
    private final List<String> bulkInstructions;
    private final Header[] headers;
    private final int documentsToCache;
    private final boolean autoflush;

    @Autowired
    public ElasticsearchCachedForwarder(RestClient esRestClient, @Value("${metricpoller.endpoints:/admin/metrics}") String[] metricsEndpoints,
                                        @Value("${metricpoller.index.name:microsvcmetrics}") String indexName,
                                        @Value("${metricpoller.index.dateFormat:yyyy-MM-dd}") String indexNameDateFormat,
                                        @Value("${metricpoller.bulk.cache.documents:10}") int documentsToCache,
                                        @Value("${metricpoller.bulk.cache.autoflush:false}") Boolean autoflush) {
        this.esRestClient = esRestClient;
        this.mappings = new HashMap <>();
        //Build the mapping name based on the endpoint, and then relate them.
        for (String endpoint : metricsEndpoints) {
            this.mappings.put(endpoint, buildMappingName(endpoint));
        }
        INDEX_NAME = indexName;
        INDEX_NAME_DATE_FORMAT = indexNameDateFormat;
        bulkInstructions = new Vector<>();
        this.headers = new Header[]{new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-ndjson")};
        this.documentsToCache = documentsToCache;
        this.autoflush = autoflush;
    }

    public void cache(HashMap<String, Object> message, ServiceInstance instance, String endpoint) {
        String jsonContent = this.buildMessageFromMetrics(message, instance);
        this.bulkInstructions.add(String.format(BULK_ACTION_AND_METADATA, this.mappings.get(endpoint)));
        this.bulkInstructions.add(jsonContent);
        //we are storing 2 instructions per document: one for the action & metadata, and the other one for the entity representing the document
        if (this.autoflush && this.bulkInstructions.size() >= documentsToCache * 2) {
            this.flush();
        }
    }

    @PreDestroy
    public void flush() {
        String jsonContent = buildBulkRequest(this.bulkInstructions);
        HttpEntity entity;
        try {
            entity = new NStringEntity(jsonContent);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Error converting string entity from Json String", e);
        }
        esRestClient.performRequestAsync("POST", getIndexName() + BULK_ENDPOINT, Collections.emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                LOG.debug("Successfully submitted metrics");
            }

            @Override
            public void onFailure(Exception exception) {
                LOG.error("Error submitting metrics",exception);
            }
        }, this.headers);

        this.bulkInstructions.clear();
    }

    private static String buildMessageFromMetrics(HashMap<String, Object> metrics, ServiceInstance instance) {
        HashMap<String, Object> jsonKeyValueMap = new HashMap<>();

        metrics.forEach((key, value) -> jsonKeyValueMap.put(key + POSFIX, value));

        //Adding the metadata not present on the /metrics reponse
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        jsonKeyValueMap.put(META_KEY_TIMESTAMP, simpleDateFormat.format(new Date()));
        jsonKeyValueMap.put(META_KEY_HOST, instance.getHost());
        jsonKeyValueMap.put(META_KEY_PORT, instance.getPort());
        jsonKeyValueMap.put(META_KEY_SVC_ID, instance.getServiceId());

        return gson.toJson(jsonKeyValueMap);
    }

    private static String buildMappingName(String endpoint){
        String mapping = endpoint.startsWith("/") ? endpoint.replaceFirst("/","") : endpoint;
        return mapping.replace("/","-");
    }

    private static String getIndexName() {
        return String.format("/%s-%s/", INDEX_NAME, getCurrentLocalDateTimeStamp());
    }

    private static String getCurrentLocalDateTimeStamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern(INDEX_NAME_DATE_FORMAT));
    }

    private static String buildBulkRequest(List<String> bulkInstructions) {
        StringBuilder bulkRequest = new StringBuilder();
        for (String instruction : bulkInstructions) {
            bulkRequest.append(instruction);
            bulkRequest.append('\n');
        }
        return bulkRequest.toString();
    }
}