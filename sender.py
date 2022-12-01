import socket
import time
import json
import struct
import os


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

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect(('127.0.0.1', 10086))
    timestamp = time.time()
    send_file(s, 't.pdf', 'aaa', timestamp)



