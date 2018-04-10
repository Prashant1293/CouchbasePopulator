package edu.knoldus;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

public class Parser {
    
    public BucketSettings getBucketSettings() {
        Config conf = ConfigFactory.load("application.conf");
        
        //Create your bucket.....
        BucketSettings sampleBucket = new DefaultBucketSettings.Builder()
                .type(BucketType.COUCHBASE)
                .name(conf.getString("bucket-name"))
                .password("")
                .quota(200) // megabytes
                .replicas(1)
                .indexReplicas(true)
                .enableFlush(true)
                .build();
        
        return sampleBucket;
    }
    
    public void loadBucket(Cluster cluster) {
        
        Config conf = ConfigFactory.load("application.conf");
        
        cluster.clusterManager(conf.getString("couchbase.cluster.username"), conf.getString("couchbase.cluster.password"))
                .insertBucket(getBucketSettings());
        
        try {
            
            ObjectMapper mapper = new ObjectMapper();
            File file = new File(getClass().getClassLoader().getResource("Ports_Coordinates.json").getFile());
            JsonNode treeJson = mapper.readTree(file);
            System.out.println("tree json is: " + treeJson);
            // Open your existing bucket....
            Bucket bucket = cluster.openBucket(conf.getString("bucket-name"));
            
            if (treeJson.isArray()) {
                ArrayNode arrayNode = (ArrayNode) treeJson;
                for (int index = 0; index < arrayNode.size(); index++) {
                    JsonNode individualElement = arrayNode.get(index);
                    loadBucketData(individualElement.get("properties").get("portCode").asText(),
                            individualElement, bucket);
                }
            } else {
                loadBucketData("FeaturePort", treeJson, bucket);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void loadBucketData(String index, JsonNode portData, Bucket bucket) {
        JsonObject portGeoData = JsonObject.fromJson(portData.toString());
        bucket.upsert(JsonDocument.create(index, portGeoData));
        
        // Create a N1QL Primary Index (but ignore if it exists)
        bucket.bucketManager().createN1qlPrimaryIndex("portCode", true, false);
        
    }
    
    public Cluster couchbaseConnector() {
        Config configuration = ConfigFactory.load("application.conf");
        Cluster cluster;
        try {
            // Initialize the Connection
            cluster = CouchbaseCluster.create(configuration.getString("couchbase_contact_point_one"));
            cluster.authenticate(configuration.getString("couchbase.cluster.username"),
                    configuration.getString("couchbase.cluster.password"));
            return cluster;
        } catch (CouchbaseException ex) {
            return null;
        }
    }
    
    public static void main(String[] args) {
        Parser parser = new Parser();
        parser.loadBucket(parser.couchbaseConnector());
    }
}
