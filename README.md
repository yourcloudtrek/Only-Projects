This repository contains practical demo projects built using Google Cloud Platform (GCP) services like Pub/Sub, Dataflow, and BigQuery.

I have explained these projects step-by-step with real demos on my YouTube channel:
https://www.youtube.com/channel/UCMJMrfwknToQ6neDmBc4bpQ


These examples are designed for beginners to understand real-time data pipelines with simple and clear implementations
1) First_Project_Pipeline.py
    Apache Beam pipeline
    Runs on Dataflow
    Processes streaming data from Pub/Sub
    Demonstrates end-to-end pipeline flow

2) producer_service_deadletter_topic.py
    Simulates a data producer
    Publishes messages to Pub/Sub topic
    Used to generate test data for pipeline

3) consumer_service_deadletter_topic.py
    Subscribes to Pub/Sub messages
    Processes incoming data
    Helps verify message flow and failures

4) windiwing.py
    Custom logic for processing (windowing concept)
    Helps demonstrate how streaming data is grouped over time

