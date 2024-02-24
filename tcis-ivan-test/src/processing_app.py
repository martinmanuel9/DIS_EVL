import ast
import sys

from collections import namedtuple
from typing import Dict

KafkaData = namedtuple("KafkaData", ["host_id", "platform", "data", "layer"])


def numerate_status(data: Dict):
    status_map = {'running': 0, 'stopped': 1, 'zombie': 2, 'dead': 3}
    data["status"] = status_map.get(data.get("status"))


def split_data(data: Dict):
    for key, value in data['num_ctx_switches'].items():
        data[key] = value

    for key, value in data['cpu_times'].items():
        data[key] = value

    for key, value in data['memory_info'].items():
        data[key] = value

    for key, value in data['io_counters'].items():
        data[key] = value

    del data['io_counters']
    del data['num_ctx_switches']
    del data['memory_info']
    del data['cpu_times']


def count_data(data: Dict):
    if data['threads'] is None:
        data['threads'] = []

    if data['open_files'] is None:
        data['open_files'] = []

    thread_count = len(data['threads'])
    file_count = len(data['open_files'])
    data['open_files_num'] = file_count
    data['threads_num'] = thread_count
    del data['open_files']
    del data['threads']


def is_public_ipaddr(ip):
    """
    Private IP Address Space 
    10.0. 0.0/8 IP addresses: 10.0. 0.0 – 10.255. 255.255.
    172.16. 0.0/12 IP addresses: 172.16. 0.0 – 172.31. 255.255.
    192.168. 0.0/16 IP addresses: 192.168. 0.0 – 192.168. 255.255.

    local IP address space
    127.x.x.x
    """

    ip = list(map(int, ip.strip().split('.')[:2]))
    if ip[0] == 0: return False
    if ip[0] == 10: return False
    if ip[0] == 172 and ip[1] in range(16, 32): return False
    if ip[0] == 192 and ip[1] == 168: return False
    if ip[0] == 127: return False
    return True


def connections_data(data: Dict):
    connections = ast.literal_eval(data['connections'])
    adjusted = 10
    count = 0
    for connection in connections:
        remote = connection.get("RemoteAddr")
        # print('remote', remote, end=' ')
        if is_public_ipaddr(remote):
            count += 1
    connections_info = count * adjusted
    # print('info: ', connections_info)

    data['connections_info'] = connections_info
    del data['connections']


def drop_data(data: Dict):
    # useful_data = set(['name', 'cwd', 'memory_percent', 'nice', 'cpu_percent', 'ionice', 'status',
    #                    'num_handles', 'time', 'voluntary', 'involuntary', 'user', 'system',
    #                    'children_user', 'children_system', 'rss', 'vms', 'num_page_faults',
    #                    'peak_wset', 'wset', 'peak_paged_pool', 'paged_pool',
    #                    'peak_nonpaged_pool', 'nonpaged_pool', 'pagefile', 'peak_pagefile',
    #                    'private', 'read_count', 'write_count', 'read_bytes', 'write_bytes',
    #                    'other_count', 'other_bytes', 'threads_num', 'open_files_num',
    #                    'connections_info'])

    useful_data = set(['Description', 'ExecutablePath', 'num_handles', 'system', 
    'user', 'other_count', 'num_page_faults', 'lpeak_pagefile', 'peak_wset', 'ionice', 
    'peak_nonpaged_pool', 'vms', 'rss', 'read_bytes', 'read_count', 'write_bytes',
    'write_count', 'pagefile', 'nonpaged_pool', 'paged_pool', 'connections_info',
    'TimeStamp'])

    keys = list(data.keys())
    for param in keys:
        if param not in useful_data:
            del data[param]


def process_app(data: KafkaData):
    # print(data)
    # sys.exit()

    # numerate_status(data.data)
    # split_data(data.data)
    # count_data(data.data)
    connections_data(data.data)
    drop_data(data.data)

    # print(data.data)
