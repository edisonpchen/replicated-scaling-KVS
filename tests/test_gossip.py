import sys
import time
import unittest
import os

import requests 

# Setup:
def usage():
    print(
        f'Usage: {sys.argv[0]} local_port1:ip1:port1 local_port2:ip2:port2 [local_port3:ip3:port3...]')
    sys.exit(1)


def check_arg_count():
    if len(sys.argv) < 3:
        usage()


def parse_args():
    check_arg_count()
    local_ports = []
    view = []
    for arg in sys.argv[1:]:
        try:
            col1_idx = arg.find(':')
            local_ports.append(int(arg[:col1_idx]))
            view.append(arg[col1_idx+1:])
        except:
            usage()
    return local_ports, view


ports, view_addresses = parse_args()
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


# Bodies:


def nodes_list(ports, hosts=None):
    if hosts is None:
        hosts = ['localhost'] * len(ports)
    return [f'{h}:{p}' for h, p in zip(hosts, ports)]


def put_view_body(addresses):
    return {'view': addresses}


def causal_metadata_body(cm={}):
    return {causal_metadata_key: cm}


def causal_metadata_from_body(body):
    return body[causal_metadata_key]


def put_val_body(val, cm=None):
    body = causal_metadata_body(cm)
    body['val'] = val
    return body


class TestAssignment1(unittest.TestCase):
    def setUp(self):
        # Uninitialize all nodes:
        for h, p in zip(hosts, ports):
            delete(kvs_view_admin_url(p, h))

    def test_1_gossip(self):
        # put view into the first node
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
     
        # key 3 in node 3
        res = put(kvs_data_key_url(keys[2], ports[2], hosts[2]),
                  put_val_body(vals[2]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        os.system('docker network disconnect kv_subnet kvs-replica3')
        time.sleep(2)

        # key 1 in node 1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # key 2 in node 2
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[1], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        os.system('docker network connect kv_subnet --ip 10.10.1.3 kvs-replica3')
        time.sleep(10)

        res = get(kvs_data_url(ports[2], hosts[2]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        self.assertIn('count', body,
                      msg='Key not found in json response')
        self.assertEqual(body['count'], 3, 'Bad count')
        self.assertIn('keys', body,
                      msg='Key not found in json response')
        self.assertCountEqual(body['keys'], keys[:3], 'Bad keys')
    
    def test_delete_key_1(self):
        # put in new view
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(5)

        # key 1 in node 1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')
        
        # key 2 in node 2
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[1], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        # key 3 in node 2
        res = put(kvs_data_key_url(keys[2], ports[1], hosts[1]),
                  put_val_body(vals[2], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        time.sleep(5)
        # delete key 2 in a different node
        res = delete(kvs_data_key_url(keys[1], ports[2], hosts[2]),
                put_val_body(vals[1], cm))
        body = res.json()
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(10)

        # get key list
        res = get(kvs_data_url(ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        self.assertIn('count', body,
                      msg='Key not found in json response')
        self.assertEqual(body['count'], 2, 'Bad count')
        self.assertIn('keys', body,
                      msg='Key not found in json response')
        self.assertEqual(body['keys'], [keys[0], keys[2]], 'Bad keys')

    def test_get_delete_key_2(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(1)

        # put in key 1 node 0
        res = put(kvs_data_key_url(keys[1], ports[0], hosts[0]),
                  put_val_body(vals[1]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        time.sleep(1)

        # dlete key 1 in node 2
        res = delete(kvs_data_key_url(keys[1], ports[2], hosts[2]),
                put_val_body(vals[1], cm))
        body = res.json()
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(2)

        # get key 1 in node 0
        res0 = get(kvs_data_key_url(keys[1], ports[0], hosts[0]),
        causal_metadata_body())
        self.assertEqual(res0.status_code, 404, msg='Bad status code')

    def test_get_non_existence_key(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(1)

        res0 = get(kvs_data_key_url(keys[4], ports[0], hosts[0]),
        causal_metadata_body())
        self.assertEqual(res0.status_code, 404, msg='Bad status code')

    def test_put_key_causal(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
    
        time.sleep(1)

        # put key 0 in node 1 with val 0
        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body("888"))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        # put key 0 in node 0 with val 1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body("222", cm1))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        # put key 0 in node 0 with val 2
        res = put(kvs_data_key_url(keys[0], ports[2], hosts[2]),
                  put_val_body("555", cm1))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        # put key 1 in node 1 with val 0
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[0], cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        # get key 1 in node 1
        res = get(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  causal_metadata_body())
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], 'Bad value')

        time.sleep(3)
        # get key 0 in node 1
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  causal_metadata_body())
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], "555", 'Bad value')

        time.sleep(5)
        # delete key 1 in node 2
        res = delete(kvs_data_key_url(keys[1], ports[2], hosts[2]),
                put_val_body(vals[1], cm1))
        body = res.json()
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(2)
        # get key list
        res = get(kvs_data_url(ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        self.assertIn('count', body,
                      msg='Key not found in json response')
        self.assertEqual(body['count'], 1, 'Bad count')
        self.assertIn('keys', body,
                      msg='Key not found in json response')
        self.assertEqual(body['keys'], [keys[0]], 'Bad keys')

    def test_view_change_on_dead_node(self):
        self.assertEqual(len(view_addresses), 5, msg='need more node')
        
        # kill one of node 
        os.system('docker network disconnect kv_subnet kvs-replica3')
        
        new_view_addresses = ['10.10.1.1:8080', '10.10.1.2:8080', '10.10.1.3:8080']
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(new_view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
    
        time.sleep(1)
        
        # put in key 1 node 0
        res = put(kvs_data_key_url(keys[1], ports[0], hosts[0]),
                  put_val_body(vals[1]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        # put in key 2 node 0
        res = put(kvs_data_key_url(keys[2], ports[1], hosts[2]),
                  put_val_body(vals[2], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        # turn on dead node
        os.system('docker network connect kv_subnet --ip 10.10.1.3 kvs-replica3')
        time.sleep(10)

        count = 0
        for h, p in zip(hosts, ports):
            if count >= 2:
                break
            count+=1
            with self.subTest(host=h, port=p, verb='get'):
                res = get(kvs_view_admin_url(p, h))
                self.assertEqual(res.status_code, 200, msg='Bad status code')
                body = res.json()
                self.assertIn('view', body,
                              msg='Key not found in json response')
                self.assertEqual(body['view'], new_view_addresses,
                                 msg='Bad view')
                
        # get key list
        res = get(kvs_data_url(ports[2], hosts[2]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        self.assertIn('count', body,
                      msg='Key not found in json response')
        self.assertEqual(body['count'], 2, 'Bad count')
        self.assertIn('keys', body,
                      msg='Key not found in json response')
        self.assertCountEqual(body['keys'], [keys[1], keys[2]], 'Bad keys')
                
        


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
