import time
import logging
import csv
import json
from quixstreams import Application

FILE_NAME = 'first_data.csv'
DNS = 'kafka'
TOPIC = 'raw-transactions'
LINES_PER_PERIOD = 1000
TOTAL_DURATION_MINUTES = 120

def main():
    app = Application(
        broker_address=f"{DNS}:9093",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        try:
            with open(FILE_NAME, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Initialize counters
                total_lines_sent = 0
                start_time = time.time()

                # Loop for TOTAL_DURATION_MINUTES
                for minute in range(TOTAL_DURATION_MINUTES):
                    lines_sent_this_minute = 0
                    minute_start_time = time.time()

                    while lines_sent_this_minute < LINES_PER_PERIOD:
                        try:
                            row = next(csv_reader)
                            row['amount'] = float(row['amount'])
                            row['distance_from_home'] = float(row['distance_from_home'])
                            row['card_present'] = (row['card_present'].lower() == 'true')
                            row['high_risk_merchant'] = (row['high_risk_merchant'].lower() == 'true')
                            row['weekend_transaction'] = (row['weekend_transaction'].lower() == 'true')
                            row['is_fraud'] = (row['is_fraud'].lower() == 'true')
                        except StopIteration:
                            print("End of file reached.")
                            return

                        json_value = json.dumps(row, ensure_ascii=False)

                        producer.produce(
                            topic=TOPIC,
                            key="Transaction",
                            value=json_value,
                        )

                        lines_sent_this_minute += 1
                        total_lines_sent += 1

                    print(f"Minute {minute + 1}: Sent {lines_sent_this_minute} lines. Total: {total_lines_sent}")

                    elapsed_time = time.time() - minute_start_time
                    time_to_wait = 15 - elapsed_time
                    if time_to_wait > 0:
                        time.sleep(time_to_wait)
                    else:
                        print("Warning: Processing is slower than real-time.")

        except FileNotFoundError:
            print(f"Error: File not found at path '{FILE_NAME}'.")
        except ValueError as ve:
            print(f"Value error: {ve}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        print("COMPLETED")

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()