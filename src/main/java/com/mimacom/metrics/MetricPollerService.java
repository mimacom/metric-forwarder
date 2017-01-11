package com.mimacom.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private static final String METRICS_ENDPOINT = "/metrics";
    private static final String EXCEPTION_MSG_KEY = "exceptionMsg";
    private static final String EXCEPTION_STACKTRACE_KEY = "exceptionStacktrace";
    private static final String ERROR_KEY = "error";
    private static final String ERROR_MESSAGE = "Instance not reachable";

    private static final Logger LOG = LoggerFactory.getLogger(MetricPollerService.class);

    private final DiscoveryClient discoveryClient;
    private final RestTemplate restTemplate;
    private final Forwarder forwarder;

    @Autowired
    public MetricPollerService(@SuppressWarnings("SpringJavaAutowiringInspection") DiscoveryClient discoveryClient, RestTemplate restTemplate, Forwarder forwarder) {
        this.discoveryClient = discoveryClient;
        this.restTemplate = restTemplate;
        this.forwarder = forwarder;
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
                LOG.debug("Processing instance #{}", count++);
                //Get the metrics and delegate the forwarding of the message
                this.forwarder.submit(this.getMetrics(instance), instance);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, Object> getMetrics(ServiceInstance instance) {
        try {
            //use the REST template to get the metrics
            return this.restTemplate.getForObject(buildInstanceUrl(instance), HashMap.class);
        } catch (Exception ex) {
            LOG.error(MessageFormat.format("Error fetching metrics for service instance: {0} with url {1}", instance.getServiceId(), buildInstanceUrl(instance)), ex);

            HashMap<String, Object> returnValue = new HashMap<>(3);
            returnValue.put(ERROR_KEY, ERROR_MESSAGE);
            returnValue.put(EXCEPTION_MSG_KEY, ex.getMessage());
            returnValue.put(EXCEPTION_STACKTRACE_KEY, ex.getStackTrace());
            return returnValue;
        }
    }

    private static String buildInstanceUrl(ServiceInstance instance) {
        return instance.getUri() + METRICS_ENDPOINT;
    }

}