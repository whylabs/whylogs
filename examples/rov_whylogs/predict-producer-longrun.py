import json
import joblib
import pandas as pd
from model_features import features,fault_features
import pickle
import numpy as np
from kafkaConnector import create_producer,create_consumer
import os
# Loading regression model
model_folder = "model_files"
reg_model = joblib.load(open(os.path.join(model_folder,'BL_GYROZ.joblib'), 'rb'))
target = 'GYROZ'

# Initializing Kafka consumer (for telemetry topic) and producker  (for prediction topic)
topic = 'telemetry-rov'
producer = create_producer()
consumer = create_consumer(topic)

def main():

    consumer.seek_to_beginning()
    session_number = 0
    prev = None
    session_data = []
    session_timeout = 10
    while True:
        record = consumer.poll(timeout_ms=session_timeout*1000, max_records=100, update_offsets=True)
        if not record:
            print("{} seconds without new data!".format(session_timeout))
        for k,v in record.items():
            for row in v:
                current = row.value
                current_ts = row.value.get("timestamp")
                res = calculate_residual(current,prev)
                to_send = {'residual':res,'timestamp':current_ts}
                producer.send('prediction-rov', to_send)
                producer.flush()
                prev = current
                prev_ts = current_ts
                finished = False

def calculate_residual(current,prev):
    if prev:
        # More than 0.5s has passed without the ROV sending new telemetry data
        if current['timestamp']-prev['timestamp']>500:
            print("OVER TIME LIMIT")
            return np.nan
        else:
            prev = { ft: float(prev[ft]) for ft in features }
            x = np.array(list(prev.values()))
            x=x.reshape(1,-1)

            # Min-max scaler for input features
            with open(os.path.join(model_folder,'BL_x.pickle'), 'rb') as f:
                scaler_x=pickle.load(f)

            x=scaler_x.transform(x)       

            py = reg_model.predict(x)
            # Min-max scaler for the target - GYROZ
            with open(os.path.join(model_folder,'BL_y.pickle'), 'rb') as f:
                scaler_y=pickle.load(f)
            py = py.reshape(1,-1)
            py = scaler_y.inverse_transform(py)
            py = py.ravel()
            y = float(current[target])
            # return residual (diff. between predicted and current)
            return abs(py[0]-y)
    else:
        return np.nan
if (__name__=='__main__'):
    main()