# Week 6 Homework
## Question  1:  Please select the statements that are correct 

1. Kafka Node is responsible to store topics
2. Zookeeper is removed form Kafka cluster starting from version 4.0
3. Retention configuration ensures the messages not get lost over specific period of time.
4. Group-Id ensures the messages are distributed to associated consumers

=> 3,4

## Question 2: Please select the Kafka concepts that support reliability and availability

1. Topic Replication
2. Topic Paritioning
3. Consumer Group Id
4. Ack All

=> 1,2,3,4

## Question 3: Please select the Kafka concepts that support scaling

1. Topic Replication
2. Topic Partitioning
3. Consumer Group Id
4. Ack All

=> 1,2,3

## Question 4: Please select the attributes that are good candidates for partitioning key. Consider cardinality of the field you have selected and scaling aspects of your application

1. payment_type
2. vendor_id
3. passenger_count
4. total_amount
5. tpep_pickup_datetime
6. tpep_dropoff_datetime

=> 5,6

## Question 5:  Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer

1. Deserializer Configuration
2. Topics Subscription
3. Bootstrap Server
4. Group-Id
5. Offset
6. Cluster Key and Cluster-Secret

=> 1,2,4,5,6

## Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.

=> (264, 780217)
