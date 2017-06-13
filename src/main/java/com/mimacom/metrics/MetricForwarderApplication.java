package com.mimacom.metrics;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

/**
 * @author Enrique Llerena Dominguez
 */
@SpringBootApplication
@EnableEurekaClient
public class MetricForwarderApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricForwarderApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    public RestClient restClient(@Value("${elasticsearch.host:localhost}") String elasticsearchHost, @Value("${elasticsearch.port:9200}") int port) {
        return RestClient.builder(new HttpHost(elasticsearchHost, port)).build();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
