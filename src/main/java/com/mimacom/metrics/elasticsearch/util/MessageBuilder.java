package com.mimacom.metrics.elasticsearch.util;

import com.google.gson.Gson;
import org.springframework.cloud.client.ServiceInstance;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by _domine3 on 23.06.2017.
 */
public class MessageBuilder {

    private static final String SUFFIX = ".value";
    private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSSZ";
    private static final String META_KEY_TIMESTAMP = "timestamp" + SUFFIX;
    private static final String META_KEY_HOST = "host" + SUFFIX;
    private static final String META_KEY_PORT = "port" + SUFFIX;
    private static final String META_KEY_SVC_ID = "serviceId" + SUFFIX;
    private static final String META_KEY_ENDPOINT_ID = "endpoint" + SUFFIX;
    private final static Gson gson = new Gson();


    public static String buildMessageFromMetrics(HashMap<String, Object> metrics, String endpoint, ServiceInstance instance) {
        HashMap<String, Object> jsonKeyValueMap = new HashMap<>();

        metrics.forEach((key, value) -> jsonKeyValueMap.put(key + SUFFIX, value));

        //Adding the metadata not present on the /metrics reponse
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        jsonKeyValueMap.put(META_KEY_TIMESTAMP, simpleDateFormat.format(new Date()));
        jsonKeyValueMap.put(META_KEY_ENDPOINT_ID, endpoint);
        jsonKeyValueMap.put(META_KEY_HOST, instance.getHost());
        jsonKeyValueMap.put(META_KEY_PORT, instance.getPort());
        jsonKeyValueMap.put(META_KEY_SVC_ID, instance.getServiceId());

        return gson.toJson(jsonKeyValueMap);
    }

}
