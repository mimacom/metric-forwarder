package com.mimacom.metrics;

import org.springframework.cloud.client.ServiceInstance;

import java.util.HashMap;

/**
 * @author Enrique Llerena Dominguez
 */
public interface Forwarder {
    void submit(HashMap<String, Object> message, ServiceInstance instance);
}