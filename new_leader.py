from datetime import datetime
import server
from server import encode_command, decode_command, encode_ping_ack, decode_ping_ack
import threading
import socket
import os
import time
import struct
import json

import numpy as np
from math import ceil
from queue import Queue, PriorityQueue
from keras.preprocessing.image import image, load_img, img_to_array
from tensorflow.keras.applications.resnet50 import ResNet50
from tensorflow.keras.applications.mobilenet import MobileNet
from tensorflow.keras.applications.imagenet_utils import preprocess_input, decode_predictions


BUFFER_SIZE = 4096
MASTER_HOST = INTRODUCER_HOST = socket.gethostbyname('fa22-cs425-3601.cs.illinois.edu')
STANDBY_HOST = socket.gethostbyname('fa22-cs425-3602.cs.illinois.edu')
MACHINE_NUM = int(socket.gethostname()[13:15])
LOG_FILEPATH = f'machine.{MACHINE_NUM}.log'
PING_PORT = 20240
MEMBERSHIP_PORT = 20241
PING_INTERVAL = 2.5
PING_TIMEOUT = 2
MASTER_PORT = 20086
FILE_PORT = 10086
GET_ADDR_PORT = 10087

def send_file(conn: socket.socket, localfilepath, sdfsfileid, timestamp):
    header_dic = {
        'sdfsfileid': sdfsfileid,  # 1.txt
        'timestamp': timestamp,
        'file_size': os.path.getsize(localfilepath)
    }
    header_json = json.dumps(header_dic)
    header_bytes = header_json.encode()
    conn.send(struct.pack('i', len(header_bytes)))
    conn.send(header_bytes)
    with open(localfilepath, 'rb') as f:
        for line in f:
            conn.send(line)

def receive_file(conn: socket.socket):
    obj = conn.recv(4)
    header_size = struct.unpack('i', obj)[0]
    header_bytes = conn.recv(header_size)
    header_json = header_bytes.decode()
    header_dic = json.loads(header_json)
    total_size, sdfsfileid, timestamp = header_dic['file_size'], header_dic['sdfsfileid'], header_dic['timestamp']

    data = b''
    recv_size = 0
    while recv_size < total_size:
        line = conn.recv(BUFFER_SIZE)
        data += line
        recv_size += len(line)
    return data, sdfsfileid, timestamp

class FServer(server.Node):
    class FileTable:
        def __init__(self):
            self.file_lookup = {}
            self.file_lookup_lock = threading.Lock()

        # insert into arr, timestamp, in order
        def _insert(self, arr, e):
            l, r = 0, len(arr)
            while l<r:
                mid=(l+r)//2
                if arr[mid] >= e:
                    r = mid
                else:
                    l = mid + 1
            arr.insert(l, e)

        # insert filename-timestamp 
        def insert_file(self, file, sdfsfileid, timestamp):
            self.file_lookup_lock.acquire()
            self.file_lookup.setdefault(sdfsfileid, [])
            self._insert(self.file_lookup[sdfsfileid], timestamp)
            self.file_lookup_lock.release()
            sdfsfilename = sdfsfileid + '-' + str(timestamp)
            with open(sdfsfilename, 'wb') as f:
                f.write(file)

        # remove file from filesystem 
        def delete_file(self, sdfsfileid):
            self.file_lookup_lock.acquire()
            self.file_lookup.pop(sdfsfileid)
            self.file_lookup_lock.release()
            # delete file

        # check if file is in filesystem
        def check_file(self, sdfsfileid):
            if sdfsfileid not in self.file_lookup:
                return None
            return self.file_lookup[sdfsfileid][-1]

        # print files
        def show_file(self):
            print('files stored at this machine:')
            self.file_lookup_lock.acquire()
            for i in self.file_lookup.keys():
                print(' ', i)
            self.file_lookup_lock.release()

        # return the first n versions 
        def get_n_versions(self, sdfsfileid, n):
            self.file_lookup_lock.acquire()
            n = min(n, len(self.file_lookup[sdfsfileid]))
            timestamps = [i for i in self.file_lookup[sdfsfileid][-n:]]
            self.file_lookup_lock.release()

            data = []
            for t in timestamps:
                with open(sdfsfileid+'-'+str(t), 'rb') as f:
                    data.append(f.read())
            return data

    def ping_thread(self):
        # generate ping_id
        ping_id = self.id + '-' + str(self.ping_count)
        self.ping_count += 1
        # encode ping
        encoded_ping = encode_ping_ack(ping_id, {'type': 'ping', 'member_id': self.id})
        # initialize cache for the ping_id
        self.ack_cache_lock.acquire()
        self.ack_cache[ping_id] = set()
        self.ack_cache_lock.release()
        # transmit ping, get the ids of the member that's been pinged
        for i in range(4):
            ids = self.transmit_message(encoded_ping, self.ping_port)
        # wait for some time to receive ack
        time.sleep(self.ping_timeout)
        # get the received ack
        self.ack_cache_lock.acquire()
        ack_cache_for_this_ping_id = self.ack_cache[ping_id]
        self.ack_cache.pop(ping_id)
        self.ack_cache_lock.release()
        # check all the acks that's received
        fail_ip = []
        for id in ids:
            if id not in ack_cache_for_this_ping_id: # if an ack is not received
                fail_ip.append(id.split(':')[0]) # add to failed
                new_membership_list = self.update_membership_list(0, id) # get updated membership_list by deleting the member that's missing

                # assign unique command id
                self.command_lock.acquire()
                command_id = self.id + '-' + str(self.command_count)
                self.command_count += 1
                self.commands.add(command_id)
                self.command_lock.release()

                # encode command
                command_content = {'type': 'failed', 'content': id}
                encoded_command_tosend = encode_command(command_id, command_content)
                self.mode_lock.acquire()
                if self.debug:
                    self.mode_lock.release()
                    print("haven't receiving ack from ", id)
                    print('sending command ', command_content) # print statement for debugging
                else:
                    self.mode_lock.release()

                # transmit message, using old membership_list
                self.transmit_message(encoded_command_tosend, self.membership_port)

                # update membership list
                self.membership_lock.acquire()
                self.membership_list = new_membership_list
                self.membership_lock.release()
                self.log_generate(id, 'failed', self.membership_list)
        if fail_ip: # inform master of failed node
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(json.dumps({'command_type': 'fail_notice', 'command_content': fail_ip}).encode(), (self.master_ip, self.master_port))

    def __init__(self, ping_port: int, membership_port: int, ping_timeout: int, ping_interval: int, log_filepath: str, file_port: int, master_port: int, master_host: str):
        super().__init__(ping_port, membership_port, ping_timeout, ping_interval, log_filepath)
        self.file_port = file_port
        self.file_cache = {}
        self.file_lock = threading.Lock()
        self.file_table = self.FileTable()
        self.put_lock = threading.Lock()
        self.get_lock = threading.Lock()
        self.put_ack_cache = {}
        self.get_ack_cache = {}
        self.ls_cache = {}
        self.ls_lock = threading.Lock()
        self.master_port = master_port
        self.master_ip = socket.gethostbyname(master_host)

        # jobs priority queue([priority, batch])
        self.job_queue = PriorityQueue()
        # batch_queue [(model, [batch])]
        self.batch_queue = Queue()
        # running_batches map(node -> [batch])
        self.running_batches = {}
        self.models = {}
        self.total_batches = 0

        # initialize locks
        self.seen_lock = threading.Lock()
        self.leader_lock = threading.Lock()
        self.members_lock = threading.Lock()
        self.batches_lock = threading.Lock()
        self.finished_lock = threading.Lock()

        self.finished = False
        self.seen_jobs = set()

        self.conf = ""
        
    # retrieves ip
    def get_ip(self, sdfsfileid):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((self.master_ip, GET_ADDR_PORT))
            except socket.error as e:
                return
            s.send(sdfsfileid.encode())
            ips = json.loads(s.recv(4096).decode())
            return ips

    # utilities
    def filehash(self, sdfsfilename):
        self.membership_lock.acquire()
        index = hash(sdfsfilename) % len(self.membership_list)
        self.membership_lock.release()
        return index

    # get the appropriate pred and successors replicas are stored at
    def getAllReplicas(self, index):
        indexes = set()
        self.membership_lock.acquire()
        l = len(self.membership_list)
        self.membership_lock.release()
        for i in range(0, 4):
            indexes.add((index + i) % l)
        self.membership_lock.acquire()
        res = [self.membership_list[i].split(':')[0] for i in indexes]
        self.membership_lock.release()
        return res

    def fileServerBackground(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.file_port))
            s.listen()
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.requestHandleThread, args=(conn, ))
                t.start()

    def requestHandleThread(self, conn: socket.socket):
        global MASTER_HOST
        command = conn.recv(BUFFER_SIZE).decode()
        if command == 'put':
            t = threading.Thread(target=self.handle_put_request, args=(conn,))
            t.start()
        elif command == 'delete':
            t = threading.Thread(target=self.handle_delete_request, args=(conn,))
            t.start()
        elif command == 'get':
            t = threading.Thread(target=self.handle_get_request, args=(conn,))
            t.start()
        elif command == 'ls':
            t = threading.Thread(target=self.handle_ls_request, args=(conn,))
            t.start()
        elif command == 'repair':
            t = threading.Thread(target=self.handle_repair_request, args=(conn,))
            t.start()
        elif command == 'replicate':
            t = threading.Thread(target=self.handle_replicate_request, args=(conn,))
            t.start()
        elif command == 'multiget':
            t = threading.Thread(target=self.handle_multiple_get_request, args=(conn,))
            t.start()
        # runs a batch by calling handle_execute
        elif command == 'executeBatch':
            t = threading.Thread(target=self.handle_execute, args=(conn,))
            t.start()
        # when finishedBatch is called, call handle_finished to update structures
        elif command == 'finishedBatch':
            t = threading.Thread(target=self.handle_finished, args=(conn,))
            t.start()
        # handle beginning of a job
        elif command == 'beginningJob':
            t = threading.Thread(target=self.handle_beginning, args=conn)
        # assign the new leader 
        elif command == 'newLeader':
            conn.send(b'1')
            decoded_command = conn.recv(BUFFER_SIZE).decode()
            MASTER_HOST = decoded_command[1]
            print("new leader:", MASTER_HOST)
        # handle the beginning of the inference
        elif command == 'beginningInference':
            t = threading.Thread(target=self.handle_beginning, args=(conn,))
            t.start()
        # when a job is finished, add it to the jobs that are done
        elif command == 'finishedJob':
            conn.send(b'1')
            if not self.job_queue.empty:
                self.seen_jobs.add(self.job_queue.get())


    # check if the all the sent ips are in the replica set, if not, handle_replicate
    def handle_repair_request(self, conn: socket.socket):
        conn.send(b'1')
        encoded_command = conn.recv(BUFFER_SIZE)
        decoded_command = json.loads(encoded_command.decode())
        sdfsfileid, ips = decoded_command['sdfsfileid'], decoded_command['ips']
        if not self.file_table.check_file(sdfsfileid):
            conn.send(b'0')
        self.membership_lock.acquire()
        index = self.membership_list.index(self.id)
        for i in range(1, 4):
            ip = self.membership_list[(i+index)%len(self.membership_list)].split(':')[0]
            if ip not in ips:
                self.handle_replicate(sdfsfileid, ip)
                conn.send(b'1')
                break
        self.membership_lock.release()
        conn.send(b'0')
        return

    # send a put command to the master for putting the file into the replica
    def handle_replicate(self, sdfsfileid, ip):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'replicate')
            s.recv(1)
            timestamp = self.file_table.check_file(sdfsfileid)
            localfilepath = sdfsfileid + '-' + str(timestamp)
            send_file(s, localfilepath, sdfsfileid, timestamp)
            s.recv(1)

            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(json.dumps({'command_type': 'put_notice', 'command_content': [sdfsfileid, ip]}).encode(), (self.master_ip, self.master_port))

    def handle_replicate_request(self, conn):
        conn.send(b'1')
        data, sdfsfileid, timestamp = receive_file(conn)
        self.file_table.insert_file(data, sdfsfileid, timestamp)
        conn.send(b'1')

    def handle_put(self, localfilepath, sdfsfileid, ip, timestamp):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'put')
            s.recv(1) # for ack
            send_file(s, localfilepath, sdfsfileid, timestamp)
            s.recv(1) # for ack
            command_id = sdfsfileid + '-' + str(timestamp)

            self.put_lock.acquire()
            self.put_ack_cache.setdefault(command_id, 0)
            self.put_ack_cache[command_id] += 1
            self.put_lock.release()

            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(json.dumps({'command_type': 'put_notice', 'command_content': [sdfsfileid, ip]}).encode(), (self.master_ip, self.master_port))

    # put the file data into filesystem with entry sdfsfileid
    def handle_put_request(self, conn: socket.socket):
        conn.send(b'1')
        data, sdfsfileid, timestamp = receive_file(conn)
        self.file_table.insert_file(data, sdfsfileid, timestamp)
        conn.send(b'1')

    # gets the file from the filesystem if timestamp is correct
    def handle_get(self, sdfsfileid, ip):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'get')
            s.recv(1)  # for ack
            s.send(sdfsfileid.encode())
            data, sdfsfileid, timestamp = receive_file(s)
            self.file_lock.acquire()
            self.file_cache.setdefault(sdfsfileid, [None, 0])
            if timestamp > self.file_cache[sdfsfileid][1]:
                self.file_cache[sdfsfileid] = [data, timestamp]
            self.file_lock.release()
            self.get_lock.acquire()
            self.get_ack_cache.setdefault(sdfsfileid, 0)
            self.get_ack_cache[sdfsfileid] += 1
            self.get_lock.release()

    # Handles get request 
    def handle_get_request(self, conn: socket.socket):
        conn.send(b'1')
        sdfsfileid = conn.recv(BUFFER_SIZE).decode()
        timestamp = self.file_table.check_file(sdfsfileid)
        sdfsfilename = sdfsfileid + '-' + str(timestamp)
        send_file(conn, sdfsfilename, sdfsfileid, timestamp)

        return

    # handles send
    def handle_send(self, command, ip):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(command[0].encode())
            s.recv(1)  # for ack
            s.send(json.dumps(command).encode())

    # handles delete 
    def handle_delete(self, sdfsfileid, ip):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'delete')
            s.recv(1)  # for ack
            s.send(sdfsfileid.encode())

    # handles delete
    def handle_delete_request(self, conn: socket.socket):
        conn.send(b'1')
        sdfsfileid = conn.recv(BUFFER_SIZE).decode()
        self.file_table.delete_file(sdfsfileid)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(json.dumps({'command_type': 'delete_notice', 'command_content': [sdfsfileid, socket.gethostbyname(socket.gethostname())]}).encode(),
                     (self.master_ip, self.master_port))
        return

    # handles ls
    def handle_ls(self, sdfsfileid, ip):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'ls')
            s.recv(1)  # for ack
            s.send(sdfsfileid.encode())
            res = s.recv(1).decode()
            if res == '1':
                self.ls_lock.acquire()
                self.ls_cache.setdefault(sdfsfileid, [])
                self.ls_cache[sdfsfileid].append(id)
                self.ls_lock.release()

    # handles ls request
    def handle_ls_request(self, conn: socket.socket):
        conn.send(b'1')
        sdfsfileid = conn.recv(BUFFER_SIZE).decode()
        exist = self.file_table.check_file(sdfsfileid)
        if exist:
            conn.send(b'1')
        else:
            conn.send(b'0')

    def handle_multiple_get(self, sdfsfileid, ip, n):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((ip, self.file_port))
            except socket.error as e:
                return
            s.send(b'multiget')
            s.recv(1) # for ack
            s.send(json.dumps({'sdfsfileid': sdfsfileid, 'n': n}).encode())
            obj = s.recv(4)
            header_size = struct.unpack('i', obj)[0]
            header_bytes = s.recv(header_size)
            header_json = header_bytes.decode()
            header_dic = json.loads(header_json)
            total_size, latest_t = header_dic['file_size'], header_dic['latest_t']
            data = b''
            recv_size = 0
            while recv_size < total_size:
                frag = s.recv(BUFFER_SIZE)
                data += frag
                recv_size += len(frag)
            k = sdfsfileid + '-' + str(n)
            self.file_lock.acquire()
            self.file_cache.setdefault(k, [None, 0])
            if self.file_cache[k][1] < latest_t:
                self.file_cache[k][0] = data
            self.file_lock.release()

            self.get_lock.acquire()
            self.get_ack_cache.setdefault(k, 0)
            self.get_ack_cache[k] += 1
            self.get_lock.release()

    # handles multiple get
    def handle_multiple_get_request(self, conn: socket.socket):
        conn.send(b'1')
        decoded = json.loads(conn.recv(BUFFER_SIZE).decode())
        sdfsfileid, n = decoded['sdfsfileid'], decoded['n']
        data = self.file_table.get_n_versions(sdfsfileid, n)
        delimiter = b'\n\n-----------------------\n\n'
        data = delimiter.join(data)
        header_dic = {
            'file_size': len(data),
            'latest_t': self.file_table.check_file(sdfsfileid)
        }
        header_json = json.dumps(header_dic)
        header_bytes = header_json.encode()
        conn.send(struct.pack('i', len(header_bytes)))
        conn.send(header_bytes)
        conn.send(data)

    # executes the batch, called when "executeBranch" is called
    def handle_execute(self, conn: socket.socket):
        conn.send(b'1')
        command, input_file, model_name, indices = json.loads(conn.recv(BUFFER_SIZE).decode())
        start, end = indices

        # update seen_jobs with input_file
        with self.seen_lock:
            if input_file not in self.seen_jobs:
                self.seen_jobs.add(input_file)

        with open(input_file, "r") as i_f:
            mode = "a"
            if model_name not in self.models:
                # initialize models
                if model_name == 'resnet':
                    self.models[model_name] = ResNet50(weights='imagenet')
                elif model_name == 'mobilenet':
                    self.models[model_name] = MobileNet(weights='imagenet')

            model = self.models[model_name]
            predictions = []
            with open(f"{self.host}-{input_file}", mode) as o_f:
                for _ in range(start-1):
                    i_f.readline()

                start_time = datetime.now()
                for _ in range(start, end, 1):
                    line = i_f.readline().strip().split(",")
                    dim_ct = int(float(line[0])//1)

                    dims = []

                    for i in range(dim_ct):
                        dims.append(int(float(line[i+1])//1))
                    
                    data = np.array(line[dim_ct+1:]).astype(np.float64)
                    data = np.reshape(data, tuple(dims))
                    x = preprocess_input(np.array([data]))
                    preds = model.predict(x)
                    predictions.append(decode_predictions(preds, top=1)[0][0][1])
        
        # when the batch has finished executing, send out a finished batch command
        cmd = ["finishedBatch", self.host, predictions, int((1000 * (datetime.now() - start_time).total_seconds())), end - start]
        self.handle_send(cmd, MASTER_HOST)
        print("finished with local batch!")

    # when a node is done working on its batch, remove it from running_batches and call reassign
    def handle_finished(self, conn: socket.socket):
        conn.send(b'1')
        _, finished_ip, predictions, time, query_size = json.loads(conn.recv(BUFFER_SIZE).decode())
        with self.batches_lock:
            self.total_batches += 1
            c = "\n"
            print(f"output from {finished_ip}:{predictions}\naverage ms per query: {time/query_size}. total batches processed = {self.total_batches}")
            if finished_ip in self.running_batches:
                self.running_batches.pop(finished_ip)
        self.reassign()

    # handle node failure and reassign jobs if a node fails; assign batches to nodes 
    def reassign(self):
        with self.members_lock:
            if len(self.membership_list) == 0:
                print("no workers found! exiting")
                exit(1)

            # handle failure
            failed = set()
            with self.batches_lock:
                working_nodes = set(self.running_batches.keys())
                alive_nodes = set(map(lambda x: x.split(":")[0], self.membership_list))
                failed = working_nodes - alive_nodes
                for i in failed:
                    self.batch_queue.put(self.running_batches[i])
                    self.running_batches.pop(i)

                # assign batches to nodes
                for m in self.membership_list:
                    if m not in self.running_batches:
                        host = m.split(":")[0]
                        # don't assign a batch to a node if that node is the master or hotstandby
                        if host == self.master_ip or host == STANDBY_HOST:
                            continue
                        
                        if not self.batch_queue.empty():
                            input_file, model_name, indices = self.batch_queue.get()
                            self.running_batches[host] = (input_file, model_name, indices)
                            self.handle_send(["executeBatch", input_file, model_name, indices], host)
                        else:
                            with self.finished_lock:
                                finished = True

    # handles the inference, gets each value passed into the input, adds values to batch_queue to be processed, reassigns the batches
    def handle_inference(self, config):
        print("in inference!")
        with open(config, "r") as f:
            jobs = json.loads(f.read())["data"]

            if self.host != STANDBY_HOST:
                self.handle_send(["beginningInference", config], STANDBY_HOST)

            # handle input requirements
            for job in jobs:
                name = job["name"]
                enabled = job["enabled"]
                priority = job.get("priority", 1000)
                batch_size = job.get("batch-size", 1)
                num_queries = job.get("num-queries", 1)
                model_name = job.get("model", None)
                input_source = job.get("input-source", None)

                if not enabled or not input_source or not model_name or num_queries == 0 or batch_size == 0:
                    continue

                self.job_queue.put((priority, name, job))

            print("put em all!")
            while not self.job_queue.empty():
                with self.finished_lock:
                    self.finished = False
                priority, name, job = self.job_queue.get()
                print(name)
                if name in self.seen_jobs:
                    if self.job_queue.empty():
                        print("finished!")
                        exit(1)
                    priority, name, job = self.job_queue.get()
                print(f"running {job}")

                batch_size = job.get("batch-size", 1)
                num_queries = job.get("num-queries", 1)
                input_source = job.get("input-source", None)
                model_name = job.get("model", None)

                num_batches = ceil(num_queries/batch_size)

                # update batch_queue
                with self.batches_lock:
                    for start in range(num_batches):
                        idx = (start * batch_size, min((start + 1) * batch_size, num_queries))
                        self.batch_queue.put((input_source, model_name, idx))

                # reassign batches to nodes
                self.reassign()

                finished = False
                
                while not finished:
                    time.sleep(1)
                    with self.finished_lock:
                        finished = self.finished

        print("finished!")

    # gets the conf command
    def handle_beginning(self, conn:socket.socket):
        conn.send(b'1')
        decoded_command = conn.recv(BUFFER_SIZE).decode()
        self.conf = decoded_command[1]

    # put sdfsfileid localfilepath
    def put(self, localfilepath, sdfsfileid):
        ips = self.get_ip(sdfsfileid)
        if not ips:
            index = self.filehash(sdfsfileid)
            ips = self.getAllReplicas(index)
        print(ips)
        timestamp = time.time()
        for ip in ips:
            t = threading.Thread(target=self.handle_put, args = (localfilepath, sdfsfileid, ip, timestamp))
            t.start()
        i = 0
        command_id = sdfsfileid + '-' + str(timestamp)
        while i < 100:
            self.put_lock.acquire()
            self.put_ack_cache.setdefault(command_id, 0)
            cnt = self.put_ack_cache[command_id]
            self.put_lock.release()
            if cnt >= min(3, len(ips)):
                break
            time.sleep(2)
            i += 1
        print("done")
        return

    def get(self, localfilepath, sdfsfileid):
        ips = self.get_ip(sdfsfileid)
        print(len(ips))
        for ip in ips:
            t = threading.Thread(target=self.handle_get, args=(sdfsfileid, ip))
            t.start()
        i = 0

        while i < 10:
            self.get_lock.acquire()
            self.get_ack_cache.setdefault(sdfsfileid, 0)
            cnt = self.get_ack_cache[sdfsfileid]
            self.get_lock.release()
            if cnt >= min(3, len(ips)):
                break
            time.sleep(2)
            i += 1
        if i == 10:
            print('get failed.')
        else:
            self.file_lock.acquire()
            data = self.file_cache[sdfsfileid][0]
            self.file_lock.release()
            with open(localfilepath, 'wb') as f:
                f.write(data)
            print('get complete.')

    # mulitcasts out the leader
    def multicast_leader(self):
        for member in self.membership_list:
            host = member.split(":")[0]
            t = threading.Thread(target=self.handle_send, args=(["newLeader", self.host],host))
            t.start()

    # checks if the leader has failed by checking if the master exists in the membership list
    def check_leader(self):
        global MASTER_HOST
        while True:
            with self.members_lock:
                if MASTER_HOST not in set(map(lambda x: x.split(":")[0], self.membership_list)):
                    print("MASTER FAILED!")
                    threading.Thread(target=self.multicast_leader).start()
                    MASTER_HOST = self.host
                    t = threading.Thread(target=self.handle_inference, args=(self.conf,))
                    t.start()
                    break
            time.sleep(1)
        return

    def run(self):
        self.join()
        t1 = threading.Thread(target=self.fileServerBackground)
        t1.start()

        if self.host == STANDBY_HOST:
            t2 = threading.Thread(target=self.check_leader)
            t2.start()

        while True:
            command = input('>')
            parsed_command = command.split()
            start_time = time.time()
            if parsed_command[0] == 'put':
                localfilepath, sdfsfileid = parsed_command[1], parsed_command[2]
                self.put(localfilepath, sdfsfileid)
            elif parsed_command[0] == 'get':
                sdfsfileid, localfilepath = parsed_command[1], parsed_command[2]
                self.get(localfilepath, sdfsfileid)
            elif parsed_command[0] == 'delete':
                sdfsfileid = parsed_command[1]
                ips = self.get_ip(sdfsfileid)
                for ip in ips:
                    t = threading.Thread(target=self.handle_delete, args=(sdfsfileid, ip))
                    t.start()
            elif parsed_command[0] == 'store':
                self.file_table.show_file()
            elif parsed_command[0] == 'ls':
                sdfsfileid = parsed_command[1]
                ips = self.get_ip(sdfsfileid)
                print('the file exists in:')
                for ip in ips:
                    print(' ', ip)
            elif parsed_command[0] == 'get_versions':
                sdfsfileid, num_versions, localfilepath = parsed_command[1], int(parsed_command[2]), parsed_command[3]
                ips = self.get_ip(sdfsfileid)
                for ip in ips:
                    self.handle_multiple_get(sdfsfileid, ip, num_versions)
                i = 0
                while i < 10:
                    k = sdfsfileid + '-' + str(num_versions)
                    self.get_lock.acquire()
                    self.get_ack_cache.setdefault(k, 0)
                    cnt = self.get_ack_cache[k]
                    self.get_lock.release()
                    if cnt >= 3:
                        break
                    time.sleep(2)
                    i += 1
                if i == 10:
                    print('get failed.')
                else:
                    self.file_lock.acquire()
                    data = self.file_cache[k][0]
                    self.file_lock.release()
                    with open(localfilepath, 'wb') as f:
                        f.write(data)
                    print('get complete.')
            # command to run the inference
            elif parsed_command[0] == 'inference':
                config = parsed_command[1]
                t = threading.Thread(target=self.handle_inference, args=(config,))
                t.start()
            
            elif command == 'leave':
                # create command id
                self.command_lock.acquire()
                command_id = self.id + '-' + str(self.command_count)
                self.command_count += 1
                self.commands.add(command_id)
                self.command_lock.release()

                # encode command
                command_content = {'type': 'leave', 'content': self.id}
                encoded_command_tosend = encode_command(command_id, command_content)
                self.transmit_message(encoded_command_tosend, self.membership_port)
                self.membership_lock.acquire()
                self.membership_list = []
                self.membership_lock.release()
                self.log_generate(command_id[:-2], 'leave', self.membership_list)
                print('Leaving...')
                break


            elif command == 'list_mem':
                print('isIntroducer: ', self.isIntroducer)
                self.membership_lock.acquire()
                print(f'there are {len(self.membership_list)} member in membership_list: ')
                for member in self.membership_list:
                    print('    ', member)
                self.membership_lock.release()
                self.command_lock.acquire()
                print(f'{len(self.commands)} commands have been executed')
                self.command_lock.release()
            elif command == 'debug':
                if self.debug:
                    print('debug mode off')
                else:
                    print('debug mode on')
                    self.log_generate(self.id, command, self.membership_list)
                self.debug = not self.debug
            elif command == 'list_self':
                print(self.id)
            elif command == 'bandwidth':
                self.bytes_lock.acquire()
                b = self.bytes
                self.bytes_lock.release()
                print(b / (time.time()-self.start_time))
            elif command == 'reset_time':
                self.bytes_lock.acquire()
                self.bytes = 0
                self.start_time = time.time()
                self.bytes_lock.release()
            else:
                print('command not found!')
            end_time = time.time()
            print('time consumed: ', end_time - start_time)


if __name__ == '__main__':
    # def __init__(self, ping_port: int, membership_port: int, ping_timeout: int, ping_interval: int, log_filepath: str,
    #              file_port: int):
    server = FServer(PING_PORT, MEMBERSHIP_PORT, PING_TIMEOUT, PING_INTERVAL, LOG_FILEPATH, FILE_PORT, MASTER_PORT, MASTER_HOST)
    server.run()
