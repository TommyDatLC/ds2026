
SERVER_UPLOAD = b'\0'
SERVER_DOWNLOAD = b'\1'

port = 9999
chunk = 1024
import socket
import random
import os
def GenerateRandom():
    hash = random.getrandbits(128)
    print("hash value: %032x" % hash)
    return str(hash)

def ReceiveFileName(client : socket,isHash = True):
    filenameByte = b''
    while True:
        byte = client.recv(1)
        if (byte == b'\0'):
            break
        filenameByte += byte
    filename = filenameByte.decode('utf-8')
    print("getted filename: " + filename)
    if (isHash == False):
        return filename
    else:
        return GenerateRandom() + ' ' + filename

def recv_file_sep(filename : str, client : socket.socket ,isDownloadedFromServer = False):
    file = None
    if (isDownloadedFromServer):
        file = open("down_" + filename , "wb")
    else:
        file = open(filename,"wb")
    data = client.recv(chunk) # Hứng hàng
    while data:
        file.write(data)
        data = client.recv(chunk) 
    file.close()

def send_file_sep(filename : str , client : socket.socket ):
    if (not os.path.isfile(filename)):
        print("file not exists")
        return
    file = open(filename, "rb")
    data = file.read(chunk) # Cắn một miếng 1024 bytes
    while data:
        client.send(data) # Nhai và nuốt
        data = file.read(chunk) # Cắn miếng tiếp theo
    file.close()
    print("Đã gửi xong, mệt vãi nồi!")
