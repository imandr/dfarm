from socket import *
import time
from py3 import to_bytes, to_str
from SockStream import SockStream

class DataClient(object):

    def __init__(self, bcast_addr, farm_name):
        self.BroadcastAddress = bcast_addr
        self.FarmName = farm_name
        self.MyHost = gethostbyname(gethostname())
        
    def init_transfer(self, bcast_msg, info, tmo):
        bsock = socket(AF_INET, SOCK_DGRAM)
        bsock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        
        t0 = time.time()
        ctl_listen_sock = socket(AF_INET, SOCK_STREAM)
        ctl_listen_sock.bind(('',0))
        ctl_listen_sock.listen(1)
        ctl_listen_port = ctl_listen_sock.getsockname()[1]

        bcast = '%s %s %d' % (bcast_msg, self.MyHost, ctl_listen_port)
        if info is not None:
            bcast += " " + info.serialize()

        peer_ctl_sock = None
        ctl_listen_sock.settimeout(1.0)
        while peer_ctl_sock is None and (tmo is None or time.time() < t0 + tmo):
            bsock.sendto(to_bytes(bcast), self.BroadcastAddress)
            try:    peer_ctl_sock, peer_ctl_addr = ctl_listen_sock.accept()
            except timeout:
                pass
            else:
                done = True
        ctl_listen_sock.close()
        bsock.close()

        if peer_ctl_sock is None:
                raise RuntimeError('Request time out')
        

        data_listen_sock = socket(AF_INET, SOCK_STREAM)
        data_listen_sock.bind((self.MyHost,0))
        data_listen_port = data_listen_sock.getsockname()[1]
        data_listen_sock.listen(1)

        ctl_str = SockStream(peer_ctl_sock)
        ctl_str.send('DATA %s %s' % (self.MyHost, data_listen_port))

        data_listen_sock.settimeout(tmo)
        try:    peer_data_sock, peer_data_addr = data_listen_sock.accept()
        except timeout:
            peer_ctl_sock.close()
            raise RuntimeError('Data connection accept() time out')
        finally:
            data_listen_sock.close()

        return  peer_ctl_sock, peer_ctl_addr,  peer_data_sock, peer_data_addr   

    def put(self, info, ppath, ncopies = 1, nolocal = True, tmo = None):
        cmd = 'ACCEPTR' if nolocal else 'ACCEPT'
                # ACCEPT <farm name> <nfrep> <lpath> <addr> <port> <info>
        bcast = '%s %s %d %s' % (cmd, self.FarmName, ncopies-1, info.Path)

        try:    peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, info, tmo)
        except Exception as e:
            return False, "Error initiating transfer: %s" % (e,)
        #print("transfer initialized")
        ok, reason = self._remote_put(peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo)

        peer_ctl_sock.close()
        return ok, reason
                
    def _remote_put(self, peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo):
        ctl_str = SockStream(peer_ctl_sock)
        eof = False
        t0 = time.time()
        nbytes = 0
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        if isinstance(ppath, str):
            fd = open(ppath, 'rb')
        else:
            fd = ppath
        with fd:
            while not eof:
                    data = fd.read(60000)
                    if not data:
                            #print ("_remote_put: eof")
                            eof = True
                    else:
                            peer_data_sock.sendall(data)
                            nbytes += len(data)
                            #print ("_remote_put: sent %d bytes" % (len(data),))
        t1 = time.time()
        peer_data_sock.shutdown(SHUT_RDWR)
        peer_data_sock.close()
        #print ("_remote_put: sending EOF...")
        answer = ctl_str.sendAndRecv('EOF %d' % (nbytes,))
        done = answer == "OK"
        size = float(nbytes)/1024.0/1024.0
        if done:
                try:    rcvr = gethostbyaddr(peer_data_addr[0])[0]
                except: rcvr = peer_data_addr[0]
                rate = ''
                if t1 >= t0 and size > 0:
                        rate = ' at %f MB/sec' % (size/(t1-t0))
                return True,'Stored %f MB on %s%s' % (size, rcvr, rate)
        else:
                return False,'Transfer aborted'


    def get(self, info, ppath, nolocal = True, tmo = None):
        cmd = 'SENDR' if nolocal else 'SEND'
        bcast = '%s %s %s %s' % (cmd, self.FarmName, info.Path, info.CTime)

        try:    peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, None, tmo)
        except Exception as e:
            return False, "Error initiating transfer: %s" % (e,)

        ok, reason = self._remote_get(peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo)

        peer_data_sock.close()
        return ok, reason


    def _remote_get(self, peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo):
        ctl_str = SockStream(peer_ctl_sock)
        eof = False
        t0 = time.time()
        nbytes = 0
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        with open(ppath, 'wb') as fd:
            while not eof:
                    #print("_remote_get: peer_data_sock.recv()...")
                    data = peer_data_sock.recv(1024*1024)
                    #print("_remote_get: peer_data_sock.recv() -> %d" % (len(data),))
                    if not data:
                            eof = True
                    else:
                            fd.write(data)
                            nbytes += len(data)
        #print("_remote_get: EOF")
        peer_data_sock.close()
        t1 = time.time()
        msg = ""
        try:
            msg = ctl_str.recv()
            #print("_remote_get: EOF message: [%s]" % (msg,))
            words = msg.split()
            assert words[0] == "EOF"
            count = int(words[1])
            #print("_remote_get: EOF received: %d" % (count,))
            ctl_str.send("OK")
        except:
            return False, "Can not parse EOF message: [%s]" % (msg,) 

        if nbytes != count:
            return False, "Incorrect byte count: EOF message: %d, actual count: %d" % (count, nbytes)

        try:    sndr = gethostbyaddr(peer_data_addr[0])[0]
        except: sndr = peer_data_addr[0]
        rate = ''
        size = float(nbytes)/1024.0/1024.0
        if t1 > t0 and size >= 0:
                rate = ' at %f MB/sec' % (size/(t1-t0))
        return True,'Reveived %f MB from %s%s' % (size, sndr, rate)

    def _local_get(self, str, fn, path):
            fr = open(path, 'r')
            fw = open(fn, 'w')
            eof = 0
            t0 = time.time()
            size = 0
            while not eof:
                    data = fr.read(100000)
                    if not data:
                            eof = 1
                    else:
                            fw.write(data)
                            size = size + len(data)
            t1 = time.time()
            rate = ''
            size = size/1024.0/1024.0
            if t1 > t0 and size >= 0:
                    rate = ' at %f MB/sec' % (size/(t1-t0))
            fr.close()
            fw.close()
            str.send('OK')
            return 1, 'Copied locally %f MB %s' % (size, rate)

    def _local_put(self, str, fn):
            if fn[0] != '/':
                    fn = os.getcwd() + '/' + fn
            self.connect()
            ans = str.sendAndRecv('COPY %s' % fn)
            self.disconnect()
            if not ans:
                    return 0, 'Transfer aborted'
            if ans == 'OK':
                    return 1, 'OK'
            else:
                    return 0, ans
