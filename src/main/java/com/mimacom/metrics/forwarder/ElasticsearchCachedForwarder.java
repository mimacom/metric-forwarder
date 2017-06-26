package com.mimacom.metrics.forwarder;

import com.mimacom.metrics.elasticsearch.util.BulkManager;
import com.mimacom.metrics.elasticsearch.util.IndexManager;
import com.mimacom.metrics.elasticsearch.util.MessageBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.Header;
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
import java.io.IOException;
import java.util.*;

/**
 * @author Enrique Llerena Dominguez
 */
@Component
public class ElasticsearchCachedForwarder {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchCachedForwarder.class);
    private final RestClient esRestClient;
    private final Map<String, String> indices;
    private final Header[] headers;
    private final BulkManager bulkManager;

    @Autowired
    public ElasticsearchCachedForwarder(RestClient esRestClient,
                                        @Value("${metricpoller.endpoints:/admin/metrics}") String[] metricsEndpoints,
                                        BulkManager bulkManager,
                                        IndexManager indexManager) {
        this.esRestClient = esRestClient;
        this.indices = new HashMap <>();
        //Build the index name based on the endpoint, and then relate them.
        for (String endpoint : metricsEndpoints) {
            this.indices.put(endpoint, indexManager.getIndexName(endpoint));
        }
        this.bulkManager = bulkManager;
        this.headers = new Header[]{this.bulkManager.getHeader()};
    }

    public void cache(HashMap<String, Object> message, ServiceInstance instance, String endpoint) throws IOException {
        String jsonContent = MessageBuilder.buildMessageFromMetrics(message, endpoint, instance);
        this.bulkManager.addInstruction(this.indices.get(endpoint), jsonContent);
        if (this.bulkManager.isAutoflush() && this.bulkManager.isCacheFull()) {
            this.flush();
        }
    }

    @PreDestroy
    public void flush() throws IOException {
        String jsonContent = this.bulkManager.getBulkRequest();
        HttpEntity entity = new NStringEntity(jsonContent);
        esRestClient.performRequestAsync("POST", BulkManager.BULK_ENDPOINT, Collections.emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                LOG.debug("Successfully submitted metrics");
            }

            @Override
            public void onFailure(Exception exception) {
                LOG.error("Error submitting metrics",exception);
            }
        }, this.headers);

        this.bulkManager.clearCachedInstructions();
    }
}