package com.mimacom.metrics;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
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
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Enrique Llerena Dominguez
 */
@Component
public class ElasticsearchForwarder implements Forwarder {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchForwarder.class);
    private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSSZ";

    private static final String POSFIX = ".value";
    private static final String META_KEY_TIMESTAMP = "timestamp" + POSFIX;
    private static final String META_KEY_HOST = "host" + POSFIX;
    private static final String META_KEY_PORT = "port" + POSFIX;
    private static final String META_KEY_SVC_ID = "serviceId" + POSFIX;


    private final Gson gson = new Gson();
    private final RestClient esRestClient;
    private final Map<String, String> mappings;

    @Autowired
    public ElasticsearchForwarder(RestClient esRestClient, @Value("${metricpoller.endpoints}") String[] metricsEndpoints) {
        this.esRestClient = esRestClient;
        this.mappings = new HashMap <>();
        Assert.notEmpty(metricsEndpoints, "At least 1 endpoint to poll is required");
        //Build the mapping name based on the endpoint, and then relate them.
        for (String endpoint : metricsEndpoints) {
            this.mappings.put(endpoint, buildMappingName(endpoint));
        }
    }

    public void submit(HashMap<String, Object> message, ServiceInstance instance, String endpoint) {
        String jsonContent = this.buildMessageFromMetrics(message, instance);
        HttpEntity entity;

        try {
            entity = new NStringEntity(jsonContent);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Error converting string entity from Json String", e);
        }
        esRestClient.performRequestAsync("POST", "/microsvcmetrics/" + this.mappings.get(endpoint), Collections.emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                LOG.debug("Successfully submitted metrics");
            }

            @Override
            public void onFailure(Exception exception) {
                LOG.error("Error submitting metrics",exception);
            }
        });
    }

    private String buildMessageFromMetrics(HashMap<String, Object> metrics, ServiceInstance instance) {
        HashMap<String, Object> jsonKeyValueMap = new HashMap<>();

        metrics.forEach((key, value) -> jsonKeyValueMap.put(key + POSFIX, value));

        //Adding the metadata not present on the /metrics reponse
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        jsonKeyValueMap.put(META_KEY_TIMESTAMP, simpleDateFormat.format(new Date()));
        jsonKeyValueMap.put(META_KEY_HOST, instance.getHost());
        jsonKeyValueMap.put(META_KEY_PORT, instance.getPort());
        jsonKeyValueMap.put(META_KEY_SVC_ID, instance.getServiceId());

        return this.gson.toJson(jsonKeyValueMap);
    }

    private static String buildMappingName(String endpoint){
        String mapping = endpoint.startsWith("/") ? endpoint.replaceFirst("/","") : endpoint;
        return mapping.replace("/","-");
    }
}