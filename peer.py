'''
***CS765 - Assignment2
Author: Satyam Behera
Develop Blockchian on P2P Network from scratch
'''

import socket
import sys
import random
import hashlib
import threading
from _thread import *
import tqdm
from time import sleep,time
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import argparse
import pickle
import signal
from io import BytesIO
from loguru import logger

import networkx as nx
import matplotlib.pyplot as plt # library to plot the Block-Chain



parser = argparse.ArgumentParser(description='Custom Blockchain')
parser.add_argument('--ip',type=str,help='Ip address of the peer')
parser.add_argument('--port',type=int,help='Mention port No of peer to run')
parser.add_argument('--iat',type=int,help='Interarrival time between the block')
parser.add_argument('--hashing_power',type=int,help='Hashing power of peer')
parser.add_argument('--f',type=int,choices=[1,2,3],help='Hashing power of peer')
args = parser.parse_args()

logger.add("logs/Block_chain_{}_{}:{}.log".format(datetime.now(),args.ip,args.port))
G=nx.Graph()

##### Variable_List #####################
serverStarted=False
connectedPeers=[]
connectedPeersDict={}
messageList={}
messageFrom={}
livenessTracker=[]
deadCounter={}
connected_seeds=[]
pending_queue=[]
longest_chain_length=0
other_won = False
is_mining = False
sync_peer=[]
synched = False
iat = args.iat 
hashing_power = args.hashing_power
no_of_nodes = 1
#################################################

conn = sqlite3.connect('Block_chain_'+str(args.port)+'.db')

conn.execute(''' CREATE TABLE IF NOT EXISTS PEER
                (ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                PREVHASH            TEXT NOT NULL,
                TIMESTAMP           REAL NOT NULL,
                MERKELROOT          TEXT NOT NULL,
                A_TIMESTAMP         REAL NOT NULL,
                HEIGHT              INT  NOT NULL,
                HASH                TEXT NOT NULL,
                PUBKEY              TEXT NOT NULL); ''')

def signal_handler(sig, frame):
    '''
    during the exit it will show the graph of chain
    @param:signal num, frame
    '''
    global G
    global no_of_nodes
    print('graph will print')
    options = {'node_size' : 1000/no_of_nodes,'font_size':250/no_of_nodes}
    nx.draw_networkx(G, pos = nx.planar_layout(G),**options)
    plt.show()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler) ## Signal handler function to print graph



class Message():
    '''
    Message format to communicate with other peer
    '''
    def __init__(self,tp,body):
        self.type = tp ## Type of message 1-block 2-liveness request 3-synch request
        self.message  = body

class Block():
    '''
    Blocks for Blackchain
    '''
    def __init__(self,ts,merkel,prevhash,height,pk):
        '''
        Block intialization
        @param: timestamp, merkel_root, and prevhash
        '''
        self.timestamp = ts
        self.merkel = merkel
        self.prevhash = prevhash
        self.height = height
        self.pk = pk
    def compute_hash(self):
        '''
        compute Hash of the block
        '''
        return hashlib.sha3_224((str(self.timestamp)+" "+str(self.merkel)+" "+str(self.prevhash)).encode('utf-8')).hexdigest()[-4:]

class Address():
    '''
    Address of the peer
    '''
    def __init__(self,ip,port):
        self.ip = ip
        self.port = port

def insert_to_chain(block):
    '''
    insert the block to db
    @param: block - Block Object
    '''
    global G
    global no_of_nodes
    no_of_nodes+=1
    conn = sqlite3.connect('Block_chain_'+str(args.port)+'.db')
    global longest_chain_length
    longest_chain_length = max(longest_chain_length,block.height)
    if block.height!=0:
        conn.execute(''' INSERT INTO PEER (PREVHASH,TIMESTAMP,MERKELROOT,A_TIMESTAMP,HEIGHT,HASH,PUBKEY)
                            VALUES(?,?,?,?,?,?,?)''',(block.prevhash,float(block.timestamp),block.merkel,
                            float(datetime.now().timestamp()),block.height,str(block.compute_hash()),block.pk))
    else:
        conn.execute(''' INSERT INTO PEER (PREVHASH,TIMESTAMP,MERKELROOT,A_TIMESTAMP,HEIGHT,HASH,PUBKEY)
                            VALUES(?,?,?,?,?,?,?)''',(block.prevhash,float(block.timestamp),block.merkel,
                            float(datetime.now().timestamp()),block.height,'9e1c',block.pk))

    conn.commit()
    conn.close()
    G.add_node(str(block.compute_hash()))
    G.add_edge(str(block.compute_hash()), block.prevhash)



def receive_block(signum,stack):
    '''
    signal to stop mining after receive the block
    @param: signal , stack of signal
    '''
    global is_mining
    #print("signaled")
    is_mining=False

signal.signal(signal.SIGALRM, receive_block)


def is_valid(block):
    '''
    Check wheather block is satisfying all rule
    @param :block - Block object
    @return : boolean flag
    '''
    # 1. check whether this hashvalue block exsist if(yes) discard
    # 2. retrive all the block from 1hr timespan of current time 
    # 3.  check prevhash value exsist or not
    conn = sqlite3.connect('Block_chain_'+str(args.port)+'.db')
    cur = conn.cursor()
    cur.execute(''' SELECT * FROM PEER WHERE HASH = ?''',(str(block.compute_hash()),))
    if len(cur.fetchall()) > 0 :
        conn.close()
        return False
    cur.execute(''' SELECT * FROM PEER WHERE HASH = ? AND TIMESTAMP > ?''',(block.prevhash,float(block.timestamp)-(1*60*60),))
    if len(cur.fetchall()) ==0:
        conn.close()
        return False
    else:
        conn.close()
        return True

def get_last_block():
    '''
    find the lastblock of longest to mine
    @return: hash of last block
    '''
    conn = sqlite3.connect('Block_chain_'+str(args.port)+'.db')
    cur = conn.cursor()
    cur.execute(''' SELECT HASH , HEIGHT FROM PEER WHERE HEIGHT = ? AND A_TIMESTAMP = (SELECT MIN(A_TIMESTAMP) FROM (SELECT A_TIMESTAMP FROM PEER WHERE HEIGHT = ?)) ''',(longest_chain_length,longest_chain_length,))
    ans = cur.fetchone()
    cur.close()
    conn.close()
    return ans

def mining():
    '''
    thread function run to do mining
    '''
    global pending_queue
    global other_won
    global is_mining
    while(True):
        temp_queue=pending_queue
        pending_queue=[]
        for block in temp_queue:
            if is_valid(block):
                insert_to_chain(block)
                broadcast(block)
        asic = np.random.exponential()/((1/int(iat))*(int(hashing_power)/100)) # exponetial random time to simulate mining
        print(int(asic)+1)
        is_mining = True
        logger.info("mining .............")
        signal.alarm(int(asic)+1)
        while is_mining:
            continue
        #is_mining = False
        if other_won == True:
            logger.info("other won.........")
            other_won = False
            continue
        logger.info("I won........")
        lastBlock = get_last_block()
        block = Block(str(datetime.now().timestamp()),'merkel_root',lastBlock[0],lastBlock[1]+1,str(args.port))
        insert_to_chain(block)
        broadcast(block)
    
def broadcast(block):
    '''
    Broadcast the block to all the connected peers
    @param: block - Block object
    '''
    msg = Message(1,block)
    message = pickle.dumps(msg)
    for peer in connectedPeers:
        try:
            peer.send(message)
        except:
            continue


def readAllSeeds():
    '''
    read all the available seed from config file
    '''
    seeds=[]
    with open("config.txt","r") as f:
        data=f.read()
        seeds+=list(set(data.split("\n")[:-1]))
        if "" in seeds:
            seeds.remove("")
    return seeds

def synch_reply(height):
    '''
    Process synch request
    @param: Height block
    @return list of block, whose height less than param 
    '''
    conn = sqlite3.connect('Block_chain_'+str(args.port)+'.db')
    res = []
    cur = conn.cursor()
    cur.execute(''' SELECT TIMESTAMP, MERKELROOT, PREVHASH, HEIGHT, PUBKEY FROM PEER WHERE HEIGHT < ?''',(height,))
    blocks = cur.fetchall()
    for block in blocks:
        res.append(Block(block[0],block[1],block[2],block[3],block[4]))
    conn.close()
    return res


def recieveMessages(host,port):
    '''
    Manage all the message receive by the node
    @param: host IP, and Port no
    '''
    global connectedPeers
    global livenessTracker
    global messageFrom
    global pending_queue
    global synched
    global is_mining
    global other_won
    while True:
        for peer in connectedPeers:
            peer.settimeout(10)
            data=""
            try:
                data=peer.recv(4096)
            except Exception as e:
                #logger.error(e)
                pass
            peer.settimeout(None)
            if data and data!="":
                data=BytesIO(data)
                while True:
                    try:
                        chunk = pickle.load(data)
                        if chunk.type==1: ##Handle block forward
                            if not synched:
                                sync_peer.append(peer)
                            #print("Block received: {}".format(len(pending_queue)))
                            pending_queue.append(chunk.message)
                            #print("add to pending queue")
                            if is_mining:
                                signal.alarm(0)
                                is_mining=False
                                other_won =True
                        elif chunk.type==2: ###Handle liveness request
                            try:
                                message = pickle.dumps(Message(3,Address(host,port)))
                                peer.send(message)
                            except:
                                pass
                        elif chunk.type==3: ##Handle liveness reply
                            livenessTracker.append("{}:{}".format(chunk.message.ip,chunk.message.port))
                        elif chunk.type==4: ### my address request
                            connectedPeersDict["{}:{}".format(chunk.message.ip,chunk.message.port)] = peer
                        elif chunk.type==5:               ### Sync request
                            #print("synch request recieved")
                            message  = pickle.dumps(Message(6,synch_reply(int(chunk.message))))
                            try:
                                peer.send(message)
                            except Exception as e:
                                logger.error(e)
                            #print("send...synch reply done")
                        elif chunk.type==6:
                            for block in chunk.message:
                                insert_to_chain(block)
                            synched = True
                    except Exception as e:
                        break


def peerListener(host,port):
    '''
    Accept Incoming conncetion to the peer
    @param : ip, Port no
    '''
    global serverStarted 
    global connectedPeers
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        s.bind((host,port))
    except:
        logger.critical("port is already in use")
        exit()
    #writeToFileAndPrint(host,port,"Binded to "+str(port))
    s.listen(10)
    serverStarted=True
    while True:
        c,addr=s.accept()
        connectedPeers.append(c)
    s.close()


def livenessCheck(host,port):
    '''
    Check the liveness of connected peer
    @param : ip, Port no
    '''
    global connectedPeers
    global livenessTracker
    sleep(15)
    while True:
        
        for peer in connectedPeers:
            try:
                message = pickle.dumps(Message(2,Address(host,port)))
                peer.send(message)
            except:
                pass
        sleep(13)
        logger.info(livenessTracker)
        logger.info(connectedPeersDict.keys())
        handleUnreplied(host,port)
        livenessTracker=[]

def handleUnreplied(host,port):
    '''
    Handle unreplied liveness request
    @param : ip, Port no
    '''
    global livenessTracker
    global connectedPeersDict
    global deadCounter
    tempDict= list(connectedPeersDict.keys())
    for peer in tempDict:
        if peer in livenessTracker:
            deadCounter[peer]=0
        else:
            try:
                deadCounter[peer]+=1
            except:
                deadCounter[peer]=1
        if(deadCounter[peer]>=3):
            handleDeadNode(peer,host,port)
    logger.info(deadCounter)

def handleDeadNode(peer,host,port):
    '''
    To handle the dead peer
    @param: socket of dead peer, ip, port no 
    '''
    try:
        connectedPeers.remove(connectedPeersDict[peer])
    except:
        pass
    for seed in connected_seeds:
        seed.send(("Dead Node:"+peer+":"+str(time())+":"+str(host)).encode())
    try:
        del connectedPeersDict[peer]
        del deadCounter[peer]
    except:
        pass
def dishonest():
    '''
    '''
    while True:
        block = Block(str(datetime.now().timestamp()),'invalid_block','0000',5,str(args.port))
        broadcast(block)
        #sleep(2)


####################################################################################################################

def Main():
    global connectedPeers
    global connected_seeds
    global pending_queue
    global synched
    global sync_peer
    global G
    host= args.ip
    port=int(args.port)

    start_new_thread(peerListener,(host,port,))
    peer_address=host+":"+str(port)
    peerID=str(hashlib.md5(peer_address.encode()).hexdigest())
    sleep(5)
    if not serverStarted:
        logger.critical("It took more time to bind the port or port is already is use")
        exit()
    seeds=readAllSeeds()
    available_peers=[]
    seeds=random.sample(seeds,int(len(seeds)/2)+1)
    #print(seeds)
    for seed in seeds:

        #writeToFileAndPrint(host,port,"Connectiong to "+seed)
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        seed_host=seed.split(":")[0]
        seed_port=int(seed.split(":")[1])
        try:
            s.settimeout(10)
            s.connect((seed_host,seed_port))
            s.settimeout(None)
            connected_seeds.append(s)
            #writeToFileAndPrint(host,port,"Connected to "+seed)

        except:
            #writeToFileAndPrint(host,port,"Could not connect to "+seed)
            pass

    if len(connected_seeds)==0:
        #writeToFileAndPrint(host,port,"No any seed server is online.")

        exit()
    for s in connected_seeds:
        data=s.recv(1024)
        print(data.decode())
        s.send(peerID.encode())
        s.recv(128)
        s.send(peer_address.encode())
        data=s.recv(2048).decode()
        if data!="first peer":
            available_peers+=data.split("\n")[:-1]
    available_peers=list(set(available_peers))
    if(len(available_peers)>4):
        available_peers = random.sample(available_peers,4)
    print(available_peers)
    for peer in available_peers:
        peer_host=peer.split(":")[0]
        peer_port=int(peer.split(":")[1])
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((peer_host,peer_port))
            s.settimeout(None)
            message = pickle.dumps(Message(4,Address(host,port)))
            s.send(message)
            #writeToFileAndPrint(host,port,"Connected to peer "+peer_host+":"+str(peer_port))
            connectedPeersDict[peer_host+":"+str(peer_port)]=s
            logger.info("inserted "+peer_host+":"+str(peer_port)+"from main")
            connectedPeers.append(s)
        except:
            logger.info("Peer seems to be dead")
            handleDeadNode(peer,host,port)
    start_new_thread(recieveMessages,(host,port,))
    start_new_thread(livenessCheck,(host,port,))
    if int(args.f) == 1:
        conn.execute('''INSERT INTO PEER (PREVHASH,TIMESTAMP,MERKELROOT,A_TIMESTAMP,HEIGHT,HASH, PUBKEY) VALUES('9e1c',?,'merkel_root',?,0,'9e1c',?)''',(float(datetime.now().timestamp()),float(datetime.now().timestamp()),str(args.port),))
        conn.commit()
        G.add_node('9e1c',node_color='r')
        start_new_thread(mining,())
    else:
        while True:
            if len(pending_queue) >0:
                break
        message  = pickle.dumps(Message(5,pending_queue[0].height))
        sync_peer[0].send(message)      ### Request for genius to Bk-1 block 
        logger.info("synch request sent")
        while not synched:
            continue
        logger.info("synched")
        start_new_thread(mining,())
    conn.close()
    if int(args.f)==3:
        start_new_thread(dishonest,()) # run the dishonest thread to flood other's pending queue with invalid block
    while True:
        pass 
Main()  






#TODO

####Honest#######
'''
1. Timestamp adjustment of 1hr --- DONE
2. interrupt of sleep ---- DONE
3. new peer synchronization - DONE
'''

###Dishonest



########## -- Message Type --- ###########
'''
#1 - 
'''
