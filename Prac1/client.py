import socket
import os
import Utils
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('0.0.0.0',Utils.port))
def uploadFile():
    filename = input()
    only_filename = os.path.basename(filename)
    print(only_filename)
    client.send(Utils.SERVER_UPLOAD)
    sendFileName(only_filename)
    Utils.send_file_sep(filename,client)
#downloadData
def downloadFile():
    print("Nhap ten file muon download")
    filename_input = input()
    client.send(Utils.SERVER_DOWNLOAD)
    sendFileName(filename_input)
    Utils.recv_file_sep(filename_input,client,True)


def sendFileName(fileName : str):
    client.send(fileName.encode("utf-8"))
    client.send(b'\0')

downloadFile()

# /home/tommydatlc/ds2026/1200px-Hatsune_Miku_V2.webp