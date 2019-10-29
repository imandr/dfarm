from socket import *
import time
from py3 import to_bytes, to_str
from SockStream import SockStream

class RemoteReader(object):
    
    def __init__(self, peer_ctl_sock, peer_data_sock, tmo):
        self.CtlSock = peer_ctl_sock
        self.DataSock = peer_data_sock
        self.PeerAddress = peer_data_sock.getpeername()
        self.NBytes = 0
        self.DataClosed = False
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        self.EOFReceived = False
        
    def stats(self):
        return self.PeerAddress, self.NBytes

    def __del__(self):
        self.close()

    def close(self):
        try:    self.DataSock.close()
        except: pass
        try:    self.CtlSock.close()
        except: pass

    def _checkEOF(self):
        if not self.EOFReceived:
            try:
                stream = SockStream(self.CtlSock)
                msg = stream.recv()
                words = msg.split()
                assert words[0] == "EOF"
                count = int(words[1])
                stream.send("OK")
            except:
                raise IOError("Error processing EOF message")
            finally:
                self.CtlSock.close()
            if self.NBytes != count:
                raise IOError("Incomplete file ransfer")
            self.EOFReceived = True

    def _read(self, n):
        if n <= 0:  return b''
        nread = 0
        parts = []
        while nread < n and not self.EOFReceived:
            try:    part = self.DataSock.recv(n - nread)
            except: part = b''
            if not part:
                self.DataSock.close()
                break
            parts.append(part)
            nread += len(part)
        self.NBytes += nread
        data = b''.join(parts)
        if not data:
            self._checkEOF()
        return data

    def read(self, n=None):
        if n is None:
            out = []
            eof = False
            while not eof:
                data = self._read(10000000)
                if not data:    eof = True
                else:           out.append(data)
            return b''.join(out)
        else:
            return self._read(n)

class RemoteWriter(object):
    
    def __init__(self, peer_ctl_sock, peer_data_sock, tmo):
        self.CtlSock = peer_ctl_sock
        self.DataSock = peer_data_sock
        self.PeerAddress = peer_data_sock.getpeername()
        self.NBytes = 0
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        self.Closed = False
        
    def stats(self):
        return self.PeerAddress, self.NBytes

    def write(self, data):
        self.DataSock.sendall(data)
        self.NBytes += len(data)
        
    def close(self):
        try:
                if not self.Closed:
                        self.DataSock.close()
                        stream = SockStream(self.CtlSock)
                        answer = stream.sendAndRecv('EOF %d' % (self.NBytes,))
                        self.CtlSock.close()
                        if answer != "OK":
                            raise IOError("Protocol error during closing handshake")
                        self.Closed = True

        finally:
                try:    self.DataSock.close()
                except: pass
                try:    self.CtlSock.close()
                except: pass
            
    def __del__(self):
        self.close()

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
        ctl_listen_sock.listen(0)
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
        ctl_listen_sock.shutdown(SHUT_RDWR)
        ctl_listen_sock.close()
        bsock.close()

        if peer_ctl_sock is None:
                raise RuntimeError('Request time out')
        
        data_listen_sock = socket(AF_INET, SOCK_STREAM)
        data_listen_sock.bind((self.MyHost,0))
        data_listen_port = data_listen_sock.getsockname()[1]
        data_listen_sock.listen(0)

        ctl_str = SockStream(peer_ctl_sock)
        ctl_str.send('DATA %s %s' % (self.MyHost, data_listen_port))

        data_listen_sock.settimeout(tmo)
        try:    peer_data_sock, peer_data_addr = data_listen_sock.accept()
        except timeout:
            peer_ctl_sock.close()
            raise RuntimeError('Data connection accept() time out')
        finally:
            data_listen_sock.shutdown(SHUT_RDWR)
            data_listen_sock.close()

        return  peer_ctl_sock, peer_ctl_addr,  peer_data_sock, peer_data_addr   

    def openRead(self, info, nolocal = False, tmo = None):
        cmd = 'SENDR' if nolocal else 'SEND'
        bcast = '%s %s %s %s' % (cmd, self.FarmName, info.Path, info.CTime)
        peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, None, tmo)
        return RemoteReader(peer_ctl_sock, peer_data_sock, tmo)

    def openWrite(self, info, ncopies = 1, nolocal = True, tmo = None):
        cmd = 'ACCEPTR' if nolocal else 'ACCEPT'
                # ACCEPT <farm name> <nfrep> <lpath> <addr> <port> <info>
        bcast = '%s %s %d %s' % (cmd, self.FarmName, ncopies-1, info.Path)

        peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, info, tmo)
        return RemoteWriter(peer_ctl_sock, peer_data_sock, tmo)

    def put(self, info, ppath, ncopies = 1, nolocal = True, tmo = None):
        outf = self.openWrite(info, ncopies, nolocal, tmo)
        if isinstance(ppath, str):
            fd = open(ppath, 'rb')
        else:
            fd = ppath
        with fd:
            eof = False
            while not eof:
                    data = fd.read(60000)
                    if not data:
                            #print ("_remote_put: eof")
                            eof = True
                    else:
                        outf.write(data)
        outf.close()
        return True, outf.stats()
                        
    def get(self, info, ppath, nolocal = True, tmo = None):
        readf = self.openRead(info, nolocal, tmo)
        do_close = False
        if isinstance(ppath, str):
            fd = open(ppath, 'wb')
            do_close = True
        else:
            fd = ppath
        if True:
            eof = False
            while not eof:
                    #print("_remote_get: peer_data_sock.recv()...")
                    data = readf.read(1024*1024*10)
                    #print("_remote_get: peer_data_sock.recv() -> %d" % (len(data),))
                    if not data:
                            eof = True
                    else:
                            fd.write(data)
        if do_close:
            fd.close()
        else:
            fd.flush()
        readf.close()
        return True, readf.stats()

    def ___put(self, info, ppath, ncopies = 1, nolocal = True, tmo = None):
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




    def ___get(self, info, ppath, nolocal = True, tmo = None):
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
