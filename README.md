# CrisisFlow - Smart Disaster Notification System

A real-time disaster communication framework that automatically routes emergency notifications through different processing pipelines based on disaster severity.

## What it does

CrisisFlow intelligently handles disaster notifications (fires, floods, power outages) by:
- **Classifying disasters** into high, medium, and low severity levels
- **Routing to specialized pipelines** optimized for each severity type
- **Ensuring fast response** for critical disasters (0.88 seconds average latency)

## Architecture

**High Severity Pipeline** (Apache Kafka + Flink)
- For critical disasters requiring immediate response
- Real-time streaming processing
- ~0.88 seconds latency

**Medium Severity Pipeline** (RabbitMQ + Spark) 
- For important but non-urgent disasters
- Near real-time processing with micro-batches
- ~2.5 seconds latency

**Low Severity Pipeline** (Hadoop MapReduce)
- For non-critical disasters
- Batch processing for high throughput
- ~3.5 minutes latency

## Severity Classification

Disasters are classified based on attributes like affected area, population impact, and intensity:

- **Fire**: Area size, building height, affected population
- **Flood**: Rainfall intensity, water level, affected population  
- **Power Outage**: Affected area, affected population

## Tech Stack

- Apache Kafka & Flink (high-severity)
- RabbitMQ & Spark (medium-severity) 
- Hadoop MapReduce (low-severity)
- Python for data generation and classification
- JSON format for disaster event logs

## Key Features

- Severity-aware routing for optimal resource utilization
- Real-time processing for critical disasters
- Scalable architecture handling 40-160 events per minute
- Fault-tolerant with built-in redundancy
- Performance monitoring with latency and throughput metrics

## Results

- **High-severity**: 746-970ms latency
- **Medium-severity**: 2.3-2.7s latency
- **Low-severity**: 3.2-3.6 minutes latency
- Maintains stable performance under varying loads
- Efficient resource allocation based on disaster criticality
