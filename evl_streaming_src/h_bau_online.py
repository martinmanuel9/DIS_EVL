import argparse
import os
import asyncio
import logging
import signal
import json
import pandas as pd
import traceback
import pytz
import csv
import time

from core import AsyncKafkaProducer, default_config, Consumer, consume
from processing_app import process_app
# from processing_sys import process_sys
# from processing_net import process_net
from files.raw_csv_header import app_header #, sys_header, net_header

from collections import namedtuple
from typing import Dict
from datetime import datetime
from pprint import pformat, pprint


os.environ['TZ'] = 'US/Arizona'

KafkaData = namedtuple("KafkaData", ["host_id", "platform", "data", "layer"])

# OS_SUPPORT = {"Windows", "Linux"}

PRODUCER_TOPIC = os.environ.get("PRODUCER_TOPIC", "jitc_data")
CONSUMER_TOPIC = os.environ.get("CONSUMER_TOPIC", "jitc_data")

# H_BAU_MODE 0: data collection and preprocessing
# H_BAU_MODE 1: real-time prediction

H_BAU_MODE = 1

# select the layer needed to be analyzed

# the c# version data collector only have application layer data
APPLICATION_LAYER_ANALYSIS = True


# whether save raw csv data
SAVE_RAW_APP_CSV_DATA = True

# path for raw csv data without preprocessing
raw_csv_dir = 'datasets'
app_raw_filename = 'raw_jitc.csv'

# whether rewrite
RESET_RAW_CSV = True

APPLICATION_LAYER_ANALYSIS = True

if H_BAU_MODE == 0:
    print("\n\n=============== BAU_MODE =================")
    print('Kafka data collection and pre-processing')
    print("==========================================\n\n")
    from kafka_preprocessed_data_collection_app import PreprocessingCollection as ml_app
    # from kafka_preprocessed_data_collection_sys_net import PreprocessingCollection as ml_sys_net

    APP_THRESHOLD = None
    # SYS_THRESHOLD = None
    # NET_THRESHOLD = None

if H_BAU_MODE == 1:
    print("\n\n=============== BAU_MODE =================")
    print('Kafka online learning and prediction')
    print("==========================================\n\n")
    from ML_online_app import MachineLearning as ml_app
    # from ML_online_sys_net import MachineLearning as ml_sys_net
    
    # check if I am on the evl_streaming_sr/modelsc directory
    if os.path.basename(os.getcwd()) != 'models':
        os.chdir(os.getcwd()+ '/evl_streaming_src/models')
    
    if APPLICATION_LAYER_ANALYSIS:
        APP_THRESHOLD = pd.read_csv('JITC_stream_threshold.csv')['Stream_threshold'].iloc[-1]
        print('Threshold: ', APP_THRESHOLD)
        os.chdir('..')

multi_layer_prediction = {"application layer": None, "system layer": None, "network layer": None}

# the number of processes rows scanning for generating one HBAU level sample
# Due to the high speed of c# collector, a larger window than python version is required.
PROCESS_MERGED_WIN_SIZE = 1000

logging.basicConfig(
    format="%(name)s:%(levelname)s:%(asctime)s: %(message)s",
    level=logging.WARNING,
    datefmt="%d-%b-%y %H:%M:%S",
)
handler = logging.StreamHandler()
logger = logging.getLogger("BAU_Monitor")
logger.addHandler(handler)

print('header:', app_header)


def handler(signum, frame):
    """
    handles signal interrupts so ip rules are handled
    properly
    """
    raise KeyboardInterrupt


def create_payload(prediction: Dict):
    # timestamp = datetime.utcnow().isoformat()
    #timestamp = datetime.now()      
    timestamp = datetime.now().isoformat()

    print('timestamp', timestamp)
    data = {
        "value": {
            "metadata": {"host_id": str(prediction.get("host_id"))},
            "title": "Host device is acting maliciously",
            "description": "Host system is acting outside of proper behavior",
            "severity": 4,
            "detected_at": timestamp,
            "created_at": timestamp,
            "anomaly_score": prediction.get("reconstruction_error"),
            "ontology": "software",
            "platform": prediction.get("platform")
        },
        "type": "hbau",
    }
    event = json.dumps(data).encode("utf-8")
    return event


def extract(message, first_extract):
    data = None

    if message.key() == b'application' and APPLICATION_LAYER_ANALYSIS:

        raw_csv_app_dir = os.path.join(raw_csv_dir, app_raw_filename)

        try:
            data = json.loads(message.value().decode())
            data["data"] = data.copy()
            data["computer_id"] = "1"

            # print(data["data"].keys())

            if SAVE_RAW_APP_CSV_DATA:

                if not os.path.exists(raw_csv_dir):
                    os.mkdir(raw_csv_dir)

                if first_extract[0] and RESET_RAW_CSV:
                    with open(raw_csv_app_dir, 'w') as f:
                        w = csv.DictWriter(f, app_header)
                        w.writeheader()
                        w.writerow(data["data"])
                else:
                    exist_file = os.path.isfile(raw_csv_app_dir)
                    with open(raw_csv_app_dir, 'a+') as f:
                        w = csv.DictWriter(f, app_header)
                        if not exist_file:
                            w.writeheader()
                        w.writerow(data["data"])

        except (SyntaxError, AttributeError, KeyError, json.JSONDecodeError):
            logger.exception("Invalid application message data")
            return None
        return KafkaData(host_id=data["computer_id"],
                        data=data["data"],
                        platform=data["OSVersion"],
                        layer=b"application_layer")


async def worker(name, queue, producer) -> None:
    try:
        process_merge_window = PROCESS_MERGED_WIN_SIZE
        if H_BAU_MODE == 1 and APPLICATION_LAYER_ANALYSIS:
            model_app = ml_app(threshold=APP_THRESHOLD, window=process_merge_window)
            model_app.load_models(platform="JITC")

            # model_app.load_models_attack_cls() # may not need for JITC

        elif H_BAU_MODE == 0 and APPLICATION_LAYER_ANALYSIS:
            model_app = ml_app(threshold=APP_THRESHOLD, window=process_merge_window)
        ready_count = 0
        first_ready = True
        while True:
            # print('ready count: ', ready_count)
            prediction = None
            data = await queue.get()
            process_app(data)
            ready = model_app.load_data(data.data)
            if ready and first_ready:
                prediction = model_app.process()
                first_ready = False
            elif ready_count == process_merge_window:  # non-overlap process merged window
                start_time = time.time()
                prediction = model_app.process()
                ready_count = 0
                print('start time')
                print(start_time)
                print('time cost for consumer processing one HBAU-level sample: %s seconds' % (time.time() - start_time))
            ready_count += 1

            if isinstance(prediction, Dict):

                multi_layer_prediction["application layer"] = prediction.get('prediction')

                # print("multi_layer_prediction", multi_layer_prediction, end='\n\n')

                if prediction.get('prediction_window_vote') == 1:
                    prediction["host_id"] = data.host_id
                    prediction["platform"] = data.platform
                    # threat = create_payload(prediction)
                    # logger.warning(pformat(prediction))
                    pprint(prediction)

                # producer.produce(topic="events", key="hbau", value=threat)
            queue.task_done()
            asyncio.sleep(0.01)
    except:
        traceback.print_exc()


async def pipeline(consumer_config, producer_config) -> None:
    consumer = Consumer(
        consumer_config,
        logger=logger
    )
    producer = AsyncKafkaProducer(
        producer_config
    )

    first_extract = [True, True, True]
    queue = asyncio.Queue(maxsize=10)
    tasks = []
    for i in range(1):
        name = f'worker-{i}'
        task = asyncio.create_task(
            worker(name, queue, producer)
        )
    tasks.append(task)

    async for message, err in consume(consumer, CONSUMER_TOPIC):

        logger.debug('consuming a message - queue size = %s', queue.qsize())
        if err:
            logger.error("An error occured: %s", err)
        else:
            value = extract(message, first_extract=first_extract)

            # print(value.data.keys())

            if value is None:
                continue

            if value.layer == b"application_layer":

                # application layer's process rows are required to put into the queue due to their
                # high speed transmission
                await queue.put(value)
                first_extract[0] = False

            logger.debug('Putting an item on queue - %s', queue.qsize())

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks)


def main():
    # breakpoint()
    signal.signal(signal.SIGTERM, handler)
    parser = argparse.ArgumentParser("asset_threat_monitoring")
    parser.add_argument(
        "-t", "--timer", type=int, default=5, help="Time to wait between reads"
    )
    args = parser.parse_args()
    producer_config = default_config()
    consumer_config = default_config({
        "queued.max.messages.kbytes": 128000,
        "group.id": "jitc_data"
    })
    asyncio.run(pipeline(consumer_config, producer_config))


if __name__ == "__main__":
    main()
