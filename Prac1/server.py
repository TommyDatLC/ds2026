import socket
import Utils 

def server_route(client : socket.socket):
    route_byte = client.recv(1)
    if (route_byte == Utils.SERVER_UPLOAD):
        server_upload(client)
    else:
        server_download(client)
    client.close()
    
def server_download(client : socket.socket):
    filename = Utils.ReceiveFileName(client,False)
    Utils.send_file_sep(filename,client)

def server_upload(client : socket.socket):
    filename = Utils.ReceiveFileName(client)
    Utils.recv_file_sep(filename,client)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('0.0.0.0', Utils.port)) 
server.listen()
while True:
    print("Đang ngồi hóng drama...")
    client, addr = server.accept()
    print("Ket noi thanh cong")
    server_route(client)




