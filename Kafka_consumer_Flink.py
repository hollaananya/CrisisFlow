from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common.typeinfo import Types
import json
import time

class DisasterProcessor(ProcessFunction):
    def __init__(self):
        self.start_time = time.time()
        self.throughput_start_time = time.time()  # Track event processing time
        
        # Dictionary to track count and latency for each disaster type
        self.disaster_metrics = {}

    def process_element(self, event, ctx):
        try:
            event_data = json.loads(event)
            disaster_type = event_data.get("disaster_type", "Unknown")
            affected_zone = event_data.get("affected_zone", "N/A")
            timestamp = float(event_data.get("timestamp", time.time()))

            latency = time.time() - timestamp

            # Initialize metrics for the disaster type if not present
            if disaster_type not in self.disaster_metrics:
                self.disaster_metrics[disaster_type] = {"count": 0, "total_latency": 0}

            # Update count and latency per disaster type
            self.disaster_metrics[disaster_type]["count"] += 1
            self.disaster_metrics[disaster_type]["total_latency"] += latency

            print(f"✅ Processed {disaster_type} in {affected_zone} | Latency: {latency:.2f}s")

            # Every 60 seconds, print throughput & latency metrics per disaster type
            elapsed_time = time.time() - self.start_time
            if elapsed_time >= 60:
                print("\n📊 Metrics (Last 60s) Per Disaster Type:")

                for disaster, metrics in self.disaster_metrics.items():
                    count = metrics["count"]
                    avg_latency = metrics["total_latency"] / count if count > 0 else 0
                    throughput = count / (time.time() - self.throughput_start_time)  # Events per second

                    print(f"🔹 {disaster}: {count} events | Avg Latency: {avg_latency:.2f}s | Throughput: {throughput:.2f} events/sec")

                # Reset counters
                self.disaster_metrics = {}
                self.start_time = time.time()
                self.throughput_start_time = time.time()

        except Exception as e:
            print(f"❌ Error processing event: {e} | Event: {event}")

def process_high_severity_events():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add the required JAR files
    env.add_jars("file:///home/pes2ug22cs067/flink/lib/flink-connector-kafka-3.1.0-1.18.jar")
    env.add_jars("file:///home/pes2ug22cs067/flink/lib/kafka-clients-3.6.1.jar")

    # Define Kafka Consumer Properties
    properties = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "flink_high",
        "auto.offset.reset": "earliest"
    }

    kafka_source = FlinkKafkaConsumer(
        topics="high_priority_disasters",
        properties=properties,
        deserialization_schema=SimpleStringSchema()
    )

    stream = env.add_source(kafka_source)

    # Apply processing function to track events & metrics per disaster type
    processed_stream = stream.process(DisasterProcessor(), output_type=Types.STRING())

    env.execute("High-Severity Disaster Processing")

if __name__ == "__main__":
    process_high_severity_events()

