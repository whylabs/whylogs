import json
import os

from kafkaConnector import create_producer

producer = create_producer()

foldername = "rov_data"
files = [str(i) for i in range(4)]

sessions = []
for ix, file in enumerate(files):
    with open(os.path.join(foldername, "{}_injected.json".format(file)), "r") as f:
        raw_data = json.load(f)
        sessions.append(
            [
                {
                    "mtarg1": str(x["mtarg1"]),
                    "mtarg2": str(x["mtarg2"]),
                    "mtarg3": str(x["mtarg3"]),
                    "roll": x["roll"],
                    "pitch": x["pitch"],
                    "yaw": x["yaw"],
                    "LACCX": x["LACCX"],
                    "LACCY": x["LACCY"],
                    "LACCZ": x["LACCZ"],
                    "GYROX": x["GYROX"],
                    "GYROY": x["GYROY"],
                    "GYROZ": x["GYROZ"],
                    "SC1I": x["SC1I"],
                    "SC2I": x["SC2I"],
                    "SC3I": x["SC3I"],
                    "BT1I": x["BT1I"],
                    "BT2I": x["BT2I"],
                    "vout": x["vout"],
                    "iout": x["iout"],
                    "cpuUsage": x["cpuUsage"],
                    "timestamp": x["timestamp"],
                }
                for x in raw_data["0"]["data"]
            ]
        )


total = 0
for session in sessions:
    for sample in session:
        producer.send("telemetry-rov", sample)
        producer.flush()
        total += 1
print("Total>>", total)
