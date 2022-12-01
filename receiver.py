import socket
import json
import time
import struct

BUFFER_SIZE = 4096

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
    return data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(('127.0.0.1', 10086))
    s.listen()
    while True:
        conn, addr = s.accept()
        data = receive_file(conn)
        print(len(data))
        with open('received', 'wb') as f:
            f.write(data)




