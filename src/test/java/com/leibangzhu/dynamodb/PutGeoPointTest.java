package com.leibangzhu.dynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.junit.Test;

public class PutGeoPointTest {

    @Test
    public void test(){
        put("00001",47.61121,-122.31841);
        put("00002",47.61122,-122.31842);
        put("00003",47.61123,-122.31843);
        put("00004",47.61124,-122.31844);
        put("00005",47.61125,-122.31845);
        put("00006",47.61126,-122.31846);
    }


    private void put(String devideId,double latitude,double longitude){

        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(dynamoDBClient,"device2");
        GeoDataManager dataManager = new GeoDataManager(config);

        GeoPoint geoPoint = new GeoPoint(latitude,longitude);
        // set the rangeKey to device Id
        AttributeValue rangeKeyValue = new AttributeValue().withS(devideId);

        PutPointRequest request = new PutPointRequest(geoPoint,rangeKeyValue);
        PutItemRequest putItemRequest = request.getPutItemRequest();

        // add deviceId
        AttributeValue deviceIdValue = new AttributeValue().withS(devideId);
        putItemRequest.addItemEntry("device_id",deviceIdValue);

        // add type
        AttributeValue typeValue = new AttributeValue().withN("2");
        putItemRequest.addItemEntry("type",typeValue);

        // add is_low_battery
        AttributeValue isLowBatteryValue = new AttributeValue().withN("0");
        putItemRequest.addItemEntry("is_low_battery",isLowBatteryValue);

        // add is_online
        AttributeValue isOnlineValue = new AttributeValue().withN("1");
        putItemRequest.addItemEntry("is_online",isOnlineValue);

        // add is_alert
        AttributeValue isAlertValue = new AttributeValue().withN("0");
        putItemRequest.addItemEntry("is_alert",isAlertValue);

        // add lock_status
        AttributeValue lockStatusValue = new AttributeValue().withN("0");
        putItemRequest.addItemEntry("lock_status",lockStatusValue);

        // add photo_url
        AttributeValue photoUrlValue = new AttributeValue().withS(" ");
        putItemRequest.addItemEntry("photo_url",photoUrlValue);

        // add remark
        AttributeValue remarkValue = new AttributeValue().withS(" ");
        putItemRequest.addItemEntry("remark",remarkValue);

        PutPointResult result = dataManager.putPoint(request);

    }

}
