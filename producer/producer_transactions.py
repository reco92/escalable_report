import time
import logging
from quixstreams import Application

FILE_NAME = 'first_data.csv'
DNS = '172.27.0.2'
TOPIC = 'raw_transactions'
LINES_PER_MINUTE = 1000
TOTAL_DURATION_MINUTES = 120

def main():
    app = Application(
        broker_address=f"{DNS}:9092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        try:
            with open(FILE_NAME, mode='r', encoding='utf-8') as file:
                # Skip the header
                next(file)
                
                # Initialize counters
                total_lines_sent = 0
                start_time = time.time()

                # Loop for 2 hours (120 minutes)
                for _ in range(TOTAL_DURATION_MINUTES):
                    lines_sent_this_minute = 0
                    
                    while lines_sent_this_minute < LINES_PER_MINUTE:
                        line = file.readline()
                        
                        # Break if end of file is reached
                        if not line:
                            print("End of file reached.")
                            return
                        
                        producer.produce(
                            topic=TOPIC,
                            key="Transaction",
                            value=line.strip(),
                        )
                        
                        lines_sent_this_minute += 1
                        total_lines_sent += 1
                    
                    print(f"Sent {lines_sent_this_minute} lines. Total: {total_lines_sent}")
                    
                    # Wait for the remaining time in the minute
                    elapsed_time = time.time() - start_time
                    time_to_wait = 60 - elapsed_time % 60
                    if time_to_wait > 0:
                        time.sleep(time_to_wait)
        except FileNotFoundError:
            print(f"Error: File not found at path '{FILE_NAME}'.")
        except Exception as e:
            print(f"An error occurred: {e}")
        print("COMPLETED")

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()