import ast

from collections import namedtuple
from typing import Dict

KafkaData = namedtuple("KafkaData", ["host_id", "platform", "data", "layer"])


def split_cpu_data(data: Dict):
    for key, value in data['cpu']['cpu_times'].items():
        data[key] = value
        data['sys_cpu_times_' + str(key)] = data[key]
        del data[key]

    for key, value in data['cpu']['cpu_frequency'].items():
        data[key] = value
        data['sys_cpu_frequency_' + str(key)] = data[key]
        del data[key]

    for key, value in data['cpu']['cpu_stats'].items():
        data[key] = value
        data['sys_cpu_stats_' + str(key)] = data[key]
        del data[key]

def split_memory_data(data: Dict):
    for key, value in data['memory']['virtual_memory'].items():
        data[key] = value
        data['sys_virtual_memory_' + str(key)] = data[key]
        del data[key]

    for key, value in data['memory']['swap_memory'].items():
        data[key] = value
        data['sys_swap_memory_' + str(key)] = data[key]
        del data[key]


def split_disk_data(data: Dict):
    for key, value in data['disk']['disk_usage'].items():
        data[key] = value
        data['sys_disk_usage_' + str(key)] = data[key]
        del data[key]

def drop_data(data: Dict):
    del data['cpu']
    del data['memory']
    del data['boot_time']
    del data['active_users']
    del data['disk']


def process_sys(data: KafkaData):

    split_cpu_data(data.data)
    split_memory_data(data.data)
    split_disk_data(data.data)
    drop_data(data.data)
