import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from confluent_kafka import Consumer, KafkaException
import pandas as pd
import json
import pickle

def feature_mapping(df):

    columns_to_drop =['BatchNo', 'CsName', 'ProcessId','Description', 'ExecutablePath', 
                      'ThreadCount', 'OtherTransferCount', 'Peakvms', 'PrivatePageCount', 
                      'QuotaNonPagedPoolUsage', 'QuotaPagedPoolUsage', 'ReadOperationCount',
                      'WriteOperationCount','WriteTransferCount', 'IDProcess', 'Name',
                      'IODataBytesPerSec','PercentPrivilegedTime', 'PercentProcessorTime',
                      'PercentUserTime','PrivateBytes', 'VirtualBytes', 'VirtualBytesPeak',
                      'OSVersion','UserName', 'ComputerId', 'TimeStamp',
                      'IODataOperationsPerSec','num_page_faultsPerSec','pagefilePeak','peak_wset','wsetPeak']
    df = df.drop(columns = columns_to_drop, axis=1)
    df = df.rename(columns={'PageFileUsage': 'pagefile', 
                            'lpeak_pagefile': 'peak_pagefile',
                            'ioniceBase':'ionice',
                            # 'wsetPeak':'peak_wset',
                            # 'wset':'wsetPeak',
                            })

    return(df)

def connections_data(df):
    for connection in df['connections']:
        if connection is None:
                continue
        if connection is not None:
            # print(connection)
            data = json.loads(connection)
            connections_info = 0
            characters_to_avoid = ['192.', '0.0.0.', '172.', '127.', '10.']

            for point in data:
                for key, value in point.items():
                    if isinstance(value, str) and not any(char in value for char in characters_to_avoid):
                        connections_info = connections_info + 1
            print(connections_info)
            df['connections_info'] = connections_info
            del df['connections']
    return df

def model_prediction(df):
    with open('/home/ivan/DIS_EVL/models/dis/tcis_files/rf_dis_model.pkl','rb') as pickle_file:
        rf_model = pickle.load(pickle_file)
    with open('/home/ivan/DIS_EVL/models/dis/tcis_files/scaler.pkl','rb') as pickle_file:
        scaler = pickle.load(pickle_file)
    df = scaler.transform(df)
   

    # Print the results
    print('prediction:  ', rf_model.predict(df))

def consume_messages(consumer, topic ):
    df = pd.DataFrame()
    json_data = []
    try:
        # Subscribe to the topic
        consumer.subscribe([topic])

        while True:
            # Poll for messages
            msg = consumer.poll(1.0)

            if msg is None:
                # print("No msg")
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event, not an error
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break
            
            data_dict = json.loads(msg.value().decode('utf-8'))

            # Create a DataFrame from the dictionary
            row = pd.DataFrame.from_dict(data_dict, orient='index').transpose()
            
            # Select features to be analyzed
            row = feature_mapping(row)
            df = connections_data(row)
            
            # Create and store dataset
            df = df.append(row, ignore_index=True)

            model_prediction(df)
            
            # print(df)
            #print(f"Consumed message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        df.to_csv('/home/ivan/DIS_EVL/models/dis/tcis_files/data.csv')

        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    # Replace these values with your actual Kafka broker and topic
    kafka_bootstrap_servers = '192.168.27.167:9092'
    kafka_topic = 'data_ivan_1'

    # Configure the Kafka consumer
    consumer_conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'tcis_research',
        # 'auto.offset.reset': 'earliest',  # start reading from the beginning of the topic
    }

    # Create the Kafka consumer
    kafka_consumer = Consumer(consumer_conf)

    # Consume and print messages
    consume_messages(kafka_consumer, kafka_topic)


