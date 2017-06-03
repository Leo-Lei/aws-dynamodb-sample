package com.leibangzhu.dynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import org.junit.Test;

import java.util.Iterator;

public class QueryIndex {

    @Test
    public void test(){

        AWSCredentials credentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,Constants.AWS_SECRET_KEY);
        //AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_WEST_2)
                .build();

//        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
//        dynamoDBClient.setRegion(usWest2);


//        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("device2");
        Index index = table.getIndex("device_id-index");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("device_id = :v_device_id")
//                .withNameMap(new NameMap()
//                        .with("#d", "Date"))
                .withValueMap(new ValueMap()
                        .withString(":v_device_id","00001"));

        ItemCollection<QueryOutcome> items = index.query(spec);
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next().toJSONPretty());
        }
    }
}
