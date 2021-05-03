import json
from logger_tools import log_session
from kafkaConnector import create_consumer

topic = 'telemetry-rov'
consumer = create_consumer(topic)

def main():
    consumer.seek_to_beginning()

    session_number = 0
    prev_ts = None
    total = 0
    session_data = []
    session_timeout = 10

    while True:
        finished = True
        record = consumer.poll(timeout_ms=session_timeout*1000, max_records=100, update_offsets=True)

        if not record:
            print("{} Seconds without new data!".format(session_timeout))
            print("Total points:",total)
            if session_data:
                print("Logging remaining data points into session ",session_number)
                dataset_name = "rov_telemetry_{}".format(session_number)
                log_session(dataset_name,session_data)
                
                session_data = []
                session_number+=1

        for k,v in record.items():
            for row in v:
                total+=1
                current_ts = row.value.get('timestamp',None)

                if not prev_ts:
                    prev_ts = current_ts
                
                if abs(current_ts - prev_ts) > 5*60*1000:
                    print("Logging session ",session_number)
                    dataset_name = "rov_telemetry_{}".format(session_number)
                    log_session(dataset_name,session_data)
                    
                    session_data = []
                    session_number+=1

                session_data.append(row.value)
                prev_ts = current_ts

if (__name__=='__main__'):
    main()