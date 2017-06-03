package com.leibangzhu.dynamodb;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.util.GeoTableUtil;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import org.junit.Test;

public class CreateTableTest {

    @Test
    public void createTable(){
        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"device2");

        // create table
        CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(config);
        CreateTableResult createTableResult = dynamoDBClient.createTable(createTableRequest);
    }

}
