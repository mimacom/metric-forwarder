package com.mimacom.metrics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * @author Enrique Llerena Dominguez
 */
@Configuration
@EnableScheduling
@Profile("!test")
public class EnableSchedulingConfiguration {

    MetricPollerService metricPollerService;

    @Autowired
    public EnableSchedulingConfiguration(@SuppressWarnings("SpringJavaAutowiringInspection") MetricPollerService metricPollerService){
        this.metricPollerService = metricPollerService;
    }

    @Scheduled(cron = "${job.cron.expression}")
    public void pollInstances(){
        this.metricPollerService.pollInstances();
    }

}
