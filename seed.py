import socket
from _thread import *
import threading
import sys
import hashlib
registered_peers=[]


def writeToFileAndPrint(host,port,data):
    print(data)
    with open("output.txt","a+") as f:
        f.write("Seed: "+host+":"+str(port)+" says --> "+data+"\n")

def decodeAndRemove(host,port,data):
    writeToFileAndPrint(host,port,"Recieved a dead node")
    data=data.split(":")
    #md5hash=hashlib.md5((data[1]+":"+data[2]).encode()).hexdigest()
    try:
        registered_peers.remove(data[1]+":"+data[2])
    except:
        print("could not remove"+data[1]+":"+data[2])
    print(registered_peers)
    #registered_peers.remove((data[1],int(data[2])))
    #with open("output.txt","a") as f:
    #    f.write("Seed : Removed a peer as dead peer")

def seed_server(c,addr,host,port):
    writeToFileAndPrint(host,port,"new connection request by "+addr[0]+":"+str(addr[1]))
    c.send("connected".encode())
    peerID=c.recv(1024).decode()
    c.send("OK".encode())
    peerAdd=c.recv(1024).decode()
    writeToFileAndPrint(host,port,"Registered the address:"+peerAdd) 
    
    peers=""
    for peer in registered_peers:
        peers+=peer+"\n"
    print(peers) 
    if peers!="":
        c.send(peers.encode())
    else:
        c.send("first peer".encode())
    #print("sent it")
    registered_peers.append(peerAdd)
    while True:
            data=c.recv(1024).decode()
            if not data:
                break
            if "Dead" in data:
                decodeAndRemove(host,port,data)
            print(data)
    
    c.close()

def Main():
    host=""
    port=int(sys.argv[1])
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.bind((host,port))
    s.listen(100) 
    writeToFileAndPrint(host,port,"Seed with port {port} started".format(port=port))
    with open("config.txt","a") as f:
        f.write("0.0.0.0:"+str(port)+"\n")
    while True:
        c,addr=s.accept()
        t_id=start_new_thread(seed_server,(c,addr,host,port,))
    s.close()

Main()
