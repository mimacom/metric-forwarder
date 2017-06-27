package com.mimacom.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;

/**
 * @author Enrique Llerena Dominguez
 */

@Component
public class MetricPollerService {

    private static final String EXCEPTION_MSG_KEY = "exceptionMsg";
    private static final String EXCEPTION_STACKTRACE_KEY = "exceptionStacktrace";
    private static final String ERROR_KEY = "error";
    private static final String ERROR_MESSAGE = "Instance not reachable";

    private static final Logger LOG = LoggerFactory.getLogger(MetricPollerService.class);

    private final DiscoveryClient discoveryClient;
    private final RestTemplate restTemplate;
    private final ElasticsearchCachedForwarder forwarder;
    private final String[] metricsEndpoints;
    private final boolean autoflush;


    @Autowired
    public MetricPollerService(@SuppressWarnings("SpringJavaAutowiringInspection") DiscoveryClient discoveryClient,
                               RestTemplate restTemplate,
                               ElasticsearchCachedForwarder forwarder,
                               @Value("${metricpoller.endpoints:/admin/metrics}") String[] metricsEndpoints,
                               @Value("${metricpoller.bulk.cache.autoflush:false}") Boolean autoflush) {
        this.discoveryClient = discoveryClient;
        this.restTemplate = restTemplate;
        this.forwarder = forwarder;
        this.metricsEndpoints = metricsEndpoints;
        this.autoflush = autoflush;
    }


    public void pollInstances() {
        //Get all the registered services
        List<String> services = discoveryClient.getServices();

        for (String service : services) {
            //Get all the instances of each service
            List<ServiceInstance> instances = discoveryClient.getInstances(service);

            LOG.info("Service:{}. Count of instances found {}", service, instances.size());

            int count = 1;
            for (ServiceInstance instance : instances) {
                for(String endpoint:this.metricsEndpoints) {
                    LOG.debug("Processing instance #{0}, endpoint {1}", count++, endpoint);
                    //Get the metrics and delegate the forwarding of the message
                    HashMap<String, Object> result = this.getMetrics(instance, endpoint);
                    this.forwarder.cache(result, instance, endpoint);
                }
            }
        }
        if(!this.autoflush){
            this.forwarder.flush();
        }
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, Object> getMetrics(ServiceInstance instance, String endpoint) {
            try {
                //use the REST template to get the metrics
                return this.restTemplate.getForObject(buildInstanceUrl(instance, endpoint), HashMap.class);
            } catch (Exception ex) {
                LOG.error(MessageFormat.format("Error fetching endpoint {0} for service instance: {1} with url {2}", endpoint, instance.getServiceId(), buildInstanceUrl(instance, endpoint)), ex);

                HashMap<String, Object> returnValue = new HashMap<>(3);
                returnValue.put(ERROR_KEY, ERROR_MESSAGE);
                returnValue.put(EXCEPTION_MSG_KEY, ex.getMessage());
                returnValue.put(EXCEPTION_STACKTRACE_KEY, ex.getStackTrace());
                return returnValue;
            }

    }

    private static String buildInstanceUrl(ServiceInstance instance, String endpoint) {
        return instance.getUri() + endpoint;
    }

}