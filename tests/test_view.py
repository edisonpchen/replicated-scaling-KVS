import sys
import time
import unittest
import os

import requests 

# Setup:
def usage():
    print(
        f'Usage: {sys.argv[0]} num_shards local_port1:ip1:port1 local_port2:ip2:port2 [local_port3:ip3:port3...]')
    sys.exit(1)


def check_arg_count():
    if len(sys.argv) < 3:
        usage()


unittest.TestLoader.sortTestMethodsUsing = None

def parse_args():
    check_arg_count()
    local_ports = []
    view = []
    shards = sys.argv[1]
    for arg in sys.argv[2:]:
        try:
            col1_idx = arg.find(':')
            local_ports.append(int(arg[:col1_idx]))
            view.append(arg[col1_idx+1:])
        except:
            usage()
    return local_ports, view, shards 


ports, view_addresses, num_shard = parse_args()
hosts = ['localhost'] * len(ports)
keys = ['key1', 'key2', 'key3', 'k4', 'k5']
vals = ['Value 1', 'val2', 'third_value', 'val 4', 'val 5']
causal_metadata_key = 'causal-metadata'


# Requests:


def get(url, body={}):
    return requests.get(url, json=body)


def put(url, body={}):
    return requests.put(url, json=body)


def delete(url, body={}):
    return requests.delete(url, json=body)


# URLs:


def make_base_url(port, host='localhost', protocol='http'):
    return f'{protocol}://{host}:{port}'


def kvs_view_admin_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/admin/view'

def kvs_data_key_url(key, port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data/{key}'


def kvs_data_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data'

def nodes_list(ports, hosts=None):
    if hosts is None:
        hosts = ['localhost'] * len(ports)
    return [f'{h}:{p}' for h, p in zip(hosts, ports)]

def put_view_body(addresses, shards=1):
    return {'num_shards': shards, 
            'nodes': addresses}

def causal_metadata_body(cm={}):
    return {causal_metadata_key: cm}

def causal_metadata_from_body(body):
    return body[causal_metadata_key]

def put_val_body(val, cm=None):
    body = causal_metadata_body(cm)
    body['val'] = val
    return body

class TestAssignment(unittest.TestCase):
    def setUp(self):
        for h, p in zip(hosts, ports):
            delete(kvs_view_admin_url(p,h))

    # put get view
    def test_a(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, num_shard))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(2)

        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        body = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        print(' test a:', body)
        print('\n')

        time.sleep(2)
       
        # test to see that new view has propagated throughout
        res = get(kvs_view_admin_url(ports[2], hosts[2]))
        body2 = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        self.assertEqual(body, body2, msg='not the same view')
    
    # decrease shard by 2 and only take first 3 nodes
    def test_b(self):
        view_addresses_1 = view_addresses[:3]
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses_1, str(int(num_shard) -2)))
        
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        time.sleep(2)

        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        body = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        print(' test b:', body)
        print('\n')

        # test to see that new view has propagated throughout
        res = get(kvs_view_admin_url(ports[2], hosts[2]))
        body2 = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        self.assertEqual(body, body2, msg='not the same view')

    # more shards and more nodes
    def test_c(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, str(int(num_shard) + 2)))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(2)

        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        body = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        print(' test c:', body)
        print('\n')

        # test to see that new view has propagated throughout
        res = get(kvs_view_admin_url(ports[2], hosts[2]))
        body2 = res.json()
        self.assertIn('view', body, msg='Key not found in json response')
        self.assertEqual(body, body2, msg='not the same view')
    
    

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

