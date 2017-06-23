package com.mimacom.metrics;

import com.mimacom.metrics.configuration.EnableSchedulingConfiguration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.web.client.RestTemplate;

/**
 * Created by _domine3 on 23.06.2017.
 */
@SpringBootApplication
@ComponentScan(value = {"com.mimacom.metrics.forwarder","com.mimacom.metrics.poller"})
public class MetricForwarderApplicationTestConfiguration {
    @Bean(destroyMethod = "close")
    public RestClient restClient(@Value("${elasticsearch.host:localhost}") String elasticsearchHost, @Value("${elasticsearch.port:9200}") int port) {
        return RestClient.builder(new HttpHost(elasticsearchHost, port)).build();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
