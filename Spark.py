from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import time
from collections import defaultdict

class DisasterMetricsTracker:
    def __init__(self):
        self.start_time = time.time()
        self.throughput_start_time = time.time()
        self.disaster_metrics = defaultdict(lambda: {"count": 0, "total_latency": 0})
        self.last_report_time = time.time()
    
    def process_batch(self, df, batch_id):
        if df.isEmpty():
            return
            
        # Reduced artificial delay to improve latency
        #time.sleep(0.2)  # Reduced from 1 second to 0.2 seconds
        
        # Convert DataFrame to list of dictionaries for processing
        disaster_records = df.collect()
        
        for record in disaster_records:
            try:
                # Parse the JSON string to get the full event
                event_data = json.loads(record.value)
                disaster_type = event_data.get("disaster_type", "Unknown")
                affected_zone = event_data.get("affected_zone", "N/A")
                timestamp = float(event_data.get("timestamp", time.time()))
                
                # Calculate latency (processing time - event time)
                latency = time.time() - timestamp
                
                # Update metrics for this disaster type
                self.disaster_metrics[disaster_type]["count"] += 1
                self.disaster_metrics[disaster_type]["total_latency"] += latency
                
                print(f"✅ Processed {disaster_type} in {affected_zone} | Latency: {latency:.2f}s")
                
            except Exception as e:
                print(f"❌ Error processing record: {e} | Record: {record}")
        
        # Check if 60 seconds have elapsed to report metrics
        current_time = time.time()
        if current_time - self.last_report_time >= 60:
            elapsed_time = current_time - self.start_time
            print("\n📊 Metrics (Last 60s) Per Disaster Type:")
            
            for disaster, metrics in self.disaster_metrics.items():
                count = metrics["count"]
                avg_latency = metrics["total_latency"] / count if count > 0 else 0
                throughput = count / (current_time - self.throughput_start_time)
                print(f"🔹 {disaster}: {count} events | Avg Latency: {avg_latency:.2f}s | Throughput: {throughput:.2f} events/sec")
            
            # Reset counters
            self.disaster_metrics = defaultdict(lambda: {"count": 0, "total_latency": 0})
            self.start_time = time.time()
            self.throughput_start_time = time.time()
            self.last_report_time = current_time

def process_medium_severity_events():
    # Initialize Spark session with improved latency configuration
    spark = SparkSession.builder \
        .appName("MediumSeverityDisasterProcessing") \
        .config("spark.streaming.blockInterval", "500ms") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    # Set log level to reduce console output
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from socket (connected to RabbitMQ bridge)
    socket_stream = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .option("includeTimestamp", True) \
        .load()
    
    # Process the socket stream data
    parsed_stream = socket_stream.selectExpr("CAST(value AS STRING)")
    
    # Create metrics tracker
    metrics_tracker = DisasterMetricsTracker()
    
    # Process each batch with reduced trigger interval to improve latency
    query = parsed_stream \
        .writeStream \
        .foreachBatch(metrics_tracker.process_batch) \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Print startup message
    print("🚀 Medium-Severity Disaster Processing started")
    print("⏱️ Processing with improved latency configuration")
    print("📊 Metrics will be displayed every 60 seconds")
    
    # Wait for the streaming query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    process_medium_severity_events()
