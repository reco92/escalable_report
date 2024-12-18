from datetime import datetime, timedelta
from dataclasses import dataclass
from quixstreams import Application
from datetime import datetime, timezone

def processFunction(msg):        
    timestamp = msg["timestamp"]
    utcformat = datetime.fromisoformat(timestamp).astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    msg["timestamp"] = utcformat      
    

    return msg

def main():
    app = Application(
    broker_address="kafka:9093",
    auto_offset_reset="latest",
    consumer_group="timestamp_processor"   )

    input_topic = app.topic("raw-transactions")
    output_topic = app.topic("process-transactions")

    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(processFunction)
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)



if __name__ == "__main__":   
    main()
    
