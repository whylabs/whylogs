import json
from collections import deque
from logger_tools import log_session
from kafkaConnector import create_consumer
import numpy as np

topic = 'prediction-rov'
consumer = create_consumer(topic)

def main():
    resid_window = deque(maxlen=15)
    session_number = 0
    prev_ts = None
    session_data = []
    consumer.seek_to_beginning()
    session_timeout = 10
    total = 0

    while True:
        record = consumer.poll(timeout_ms=session_timeout*1000, max_records=100, update_offsets=True)

        if not record:
            print("{} Seconds without new data!".format(session_timeout))
            print("total points>>",total)
            if session_data:
                print("Logging remaining data points into session ",session_number)
                dataset_name = "rov_prediction_{}".format(session_number)
                log_session(dataset_name,session_data)
                
                session_data = []
                session_number+=1
    
        for k,v in record.items():
            for row in v:
                total += 1
                current = row.value
                current_ts = current['timestamp']
                resid_window.append(current['residual'])
                current = add_moving_averages(resid_window,current)

                if not prev_ts:
                    prev_ts = current_ts

                if abs(current_ts - prev_ts) > 5*60*1000:
                    print("Logging session ",session_number)
                    dataset_name = "rov_prediction_{}".format(session_number)
                    log_session(dataset_name,session_data)                    
                    session_data = []
                    session_number+=1
                session_data.append(current)
                prev_ts = current_ts

def moving_average(slice_,wd):
    if np.nan in slice_:
        return np.nan
    if len(slice_) != wd:
        return np.nan
    return sum(slice_)/wd

def add_moving_averages(resid_window,current):

    r_5 = list(resid_window)[-5:]
    r_10 = list(resid_window)[-10:]
    r_15 = list(resid_window)[-15:]

    current['residual_m5'] = moving_average(r_5,5)
    current['residual_m10'] = moving_average(r_10,10)
    current['residual_m15'] = moving_average(r_15,15)

    return current


if (__name__=='__main__'):
    main()