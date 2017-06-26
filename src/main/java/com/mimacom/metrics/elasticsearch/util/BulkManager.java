package com.mimacom.metrics.elasticsearch.util;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Vector;

/**
 * Created by _domine3 on 23.06.2017.
 */
@Component
public class BulkManager {

    private final int documentsToCache;
    private final boolean autoflush;
    private static final String BULK_ACTION_AND_METADATA = "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"timestamped-metric\"} }";
    public static final String BULK_ENDPOINT = "_bulk";
    private final List<String> bulkInstructions;
    private BasicHeader header;

    @Autowired
    public BulkManager(@Value("${metricpoller.bulk.cache.documents:10}") int documentsToCache,
            @Value("${metricpoller.bulk.cache.autoflush:false}") Boolean autoflush) {
        this.documentsToCache = documentsToCache;
        this.autoflush = autoflush;
        this.bulkInstructions = new Vector<>();
        this.header = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-ndjson");
    }

    public int getDocumentsToCache() {
        return documentsToCache;
    }

    public boolean isAutoflush() {
        return autoflush;
    }

    public BasicHeader getHeader() {
        return header;
    }

    public void setHeader(BasicHeader header) {
        this.header = header;
    }


    public void addInstruction(String mapping, String jsonDocument){
        this.bulkInstructions.add(String.format(BULK_ACTION_AND_METADATA, mapping));
        this.bulkInstructions.add(jsonDocument);
    }

    public boolean isCacheFull(){
        //we are storing 2 instructions per document: one for the action & metadata, and the other one for the entity representing the document
        return this.bulkInstructions.size() >= this.getDocumentsToCache() * 2;
    }

    public String getBulkRequest() {
        StringBuilder bulkRequest = new StringBuilder();
        for (String instruction : this.bulkInstructions) {
            bulkRequest.append(instruction);
            bulkRequest.append('\n');
        }
        return bulkRequest.toString();
    }

    public void clearCachedInstructions() {
        this.bulkInstructions.clear();
    }

}
