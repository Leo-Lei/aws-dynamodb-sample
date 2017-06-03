package com.leibangzhu.dynamodb;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.*;
import com.amazonaws.geo.util.GeoTableUtil;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@SpringBootApplication
public class App {

    @RequestMapping("/")
    public String home(){
        run();
        return "Hello world!";
    }

    public static void main(String[] args) {
        SpringApplication.run(App.class,args);
    }

    private static void run(){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"test-table2");
        GeoDataManager dataManager = new GeoDataManager(config);

        // create table
//        CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(config);
//        CreateTableResult createTableResult = dynamoDBClient.createTable(createTableRequest);

        // insert a simple Geo point
        double latitude = 47.61121;
        double longitude = -122.31846;

        GeoPoint geoPoint = new GeoPoint(latitude,longitude);
        AttributeValue rangeKeyValue = new AttributeValue().withS("hello");

        PutPointRequest request = new PutPointRequest(geoPoint,rangeKeyValue);
        PutPointResult result = dataManager.putPoint(request);
        // below row will be inserted into table:
        // hashKey:609348, rangeKey: hello, geoJson: {"coordinates":[47.61121,-122.31846],"type":"Point"} , geohash: 6093487732236927555


        request = new PutPointRequest(geoPoint,rangeKeyValue);
        AttributeValue schoolNameValue = new AttributeValue().withS("Custom School Name");
        AttributeValue schoolZipValue = new AttributeValue().withN("98765");

        PutItemRequest putItemRequest = request.getPutItemRequest();
        putItemRequest.addItemEntry("schoolName",schoolNameValue);
        putItemRequest.addItemEntry("schoolZip",schoolZipValue);

        result = dataManager.putPoint(request);

        // query point
        GeoPoint centerPoint = new GeoPoint(47.61122,-122.31848);
        QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, 250);
        QueryRadiusResult queryRadiusResult = dataManager.queryRadius(queryRadiusRequest);
        for (Map<String, AttributeValue> item : queryRadiusResult.getItem()) {
            System.out.println("item: " + item);
        }


    }

    private static void createTable(String tableName){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"test-table3");
        //GeoDataManager dataManager = new GeoDataManager(config);

        // create table
        CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(config);
        CreateTableResult createTableResult = dynamoDBClient.createTable(createTableRequest);
    }

    private static void insertSimpleGeoPoint(){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"test-table2");
        GeoDataManager dataManager = new GeoDataManager(config);

        // insert a simple Geo point
        double latitude = 47.61121;
        double longitude = -122.31846;

        GeoPoint geoPoint = new GeoPoint(latitude,longitude);
        AttributeValue rangeKeyValue = new AttributeValue().withS("hello");

        PutPointRequest request = new PutPointRequest(geoPoint,rangeKeyValue);
        PutPointResult result = dataManager.putPoint(request);
        // below row will be inserted into table:
        // hashKey:609348, rangeKey: hello, geoJson: {"coordinates":[47.61121,-122.31846],"type":"Point"} , geohash: 6093487732236927555
    }

    private static void searchRadius(double latitude,long longitude,long radius){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"test-table2");
        GeoDataManager dataManager = new GeoDataManager(config);

        // query point
        GeoPoint centerPoint = new GeoPoint(47.61122,-122.31848);
        QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, 250);
        QueryRadiusResult queryRadiusResult = dataManager.queryRadius(queryRadiusRequest);
        for (Map<String, AttributeValue> item : queryRadiusResult.getItem()) {
            System.out.println("item: " + item);
        }
    }

    private static void insertGeoPoint(){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"test-table2");
        GeoDataManager dataManager = new GeoDataManager(config);

        // insert a simple Geo point
        double latitude = 47.61121;
        double longitude = -122.31846;

        GeoPoint geoPoint = new GeoPoint(latitude,longitude);
        AttributeValue rangeKeyValue = new AttributeValue().withS("hello");

        PutPointRequest request = new PutPointRequest(geoPoint,rangeKeyValue);
        AttributeValue schoolNameValue = new AttributeValue().withS("Custom School Name");
        AttributeValue schoolZipValue = new AttributeValue().withN("98765");

        PutItemRequest putItemRequest = request.getPutItemRequest();
        putItemRequest.addItemEntry("schoolName",schoolNameValue);
        putItemRequest.addItemEntry("schoolZip",schoolZipValue);

        PutPointResult result = dataManager.putPoint(request);

    }

}

