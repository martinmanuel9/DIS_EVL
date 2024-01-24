import ast

from collections import namedtuple
from typing import Dict

KafkaData = namedtuple("KafkaData", ["host_id", "platform", "data", "layer"])


def split_network_io_data(data: Dict):
    for key, value in data['network_io'].items():
        data[key] = value
        data['network_io_' + str(key)] = data[key]
        del data[key]

def network_connections(data: Dict):
    raddr_count = 0 
    laddr_count = 0
    for connection in data["connections"]:
        conns_raddr = connection.get("raddr")
        if conns_raddr:
            ip_raddr = conns_raddr.get("ip")
            if ip_raddr:
                raddr_count += 1

        conns_laddr = connection.get("laddr")
        if conns_laddr:
            ip_laddr = conns_laddr.get("ip")
            if ip_laddr:
                laddr_count += 1

    data['connections_raddr'] = raddr_count
    data['connections_laddr'] = laddr_count

def drop_data(data: Dict):

    del data['connections']
    del data['addresses']
    del data['statistics']
    del data['network_io']
    del data['hostname']


def process_net(data: KafkaData):
    network_connections(data.data)
    split_network_io_data(data.data)
    drop_data(data.data)
