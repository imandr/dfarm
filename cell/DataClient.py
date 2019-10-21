class DataClient(object):

    def __init__(self, bcast_addr, farm_name):
        self.BroadcastAddress = bcast_addr
        self.FarmName = farm_name
        self.MyHost = gethostbyname(gethostname())
        
    def init_transfer(self, bcast_msg, tmo):
        bsock = socket(AF_INET, SOCK_DGRAM)
        bsock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        
        t0 = time.time()
        ctl_listen_sock = socket(AF_INET, SOCK_STREAM)
        ctl_listen_sock.bind(('',0))
        ctl_listen_sock.listen(1)
        ctl_listen_port = ctl_listen_sock.getsockname()[1]

        bcast = '%s %s %d' % (bcast_msg, self.MyHost, ctl_listen_port)

        peer_ctl_sock = None
        ctl_listen_sock.settimeout(1.0)
        while mover_ctl_sock is None and (tmo is None or time.time() < t0 + tmo):
            bsock.sendto(to_bytes(bcast), self.BroadcastAddress)
            try:    peer_ctl_sock, peer_ctl_addr = ctl_sock.accept()
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

        ctl_str = SockStream(ctl_sock)
        ctl_str.send('DATA %s %s' % (self.MyHost, data_listen_port))

        data_listen_sock.settimeout(tmo)
        try:    peer_data_sock, peer_data_addr = data_listen_sock.accept()
        except timeout:
            peer_ctl_sock.close()
            raise RuntimeError('Data connection accept() time out')
        finally:
            data_listen_sock.close()

        return  peer_ctl_sock, peer_ctl_addr,  peer_data_sock, peer_data_addr   

    def put(self, ppath, lpath, ctime, ncopies = 1, nolocal = True, tmo = None):
        cmd = 'ACCEPTR' if nolocal else 'ACCEPT'
        bcast = '%s %s %d %s %s' % (cmd, self.FarmName, ncopies-1, lpath, ctime)

        try:    peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, tmo)
        except Exception as e:
            return False, "Error initiating transfer: %s" % (e,)
        ok, reason = self._remote_put(peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo)

        peer_ctl_sock.close()
        return ok, reason
                
    def _remote_put(self, peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, pppath, tmo):
        ctl_str = SockStream(peer_ctl_sock)
        eof = False
        t0 = time.time()
        nbytes = 0
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        with open(fn, 'rb') as fd:
            while not eof:
                    data = fd.read(60000)
                    if not data:
                            eof = False
                    else:
                            peer_data_sock.sendall(data)
                            nbytes += len(data)
        t1 = time.time()
        peer_data_sock.shutdown(SHUT_RDWR)
        peer_data_sock.close()
        answer = ctl_str.sendAndRecv('EOF %d' % (nbytes,))
        done = answer == "OK"
        size = float(size)/1024.0/1024.0
        if done:
                try:    rcvr = gethostbyaddr(peer_data_addr[0])[0]
                except: rcvr = peer_data_addr[0]
                rate = ''
                if t1 >= t0 and size > 0:
                        rate = ' at %f MB/sec' % (size/(t1-t0))
                return True,'Stored %f MB on %s%s' % (size, rcvr, rate)
        else:
                return False,'Transfer aborted'


    def get(self, lpath, ppath, info, nolocal = True, tmo = None):
        cmd = 'SENDR' if nolocal else 'SEND'
        bcast = '%s %s %s %s' % (cmd, self.FarmName, lpath, ctime)

        try:    peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr = self.init_transfer(bcast, tmo)
        except Exception as e:
            return False, "Error initiating transfer: %s" % (e,)

        ok, reason = self._remote_get(peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, ppath, tmo)

        peer_data_sock.close()
        return ok, reason


    def _remote_get(self, peer_ctl_sock, peer_ctl_addr, peer_data_sock, peer_data_addr, fn, tmo):
        ctl_str = SockStream(peer_ctl_sock)
        eof = False
        t0 = time.time()
        nbytes = 0
        if tmo is not None:
            peer_data_sock.settimeout(tmo)
        with open(fn, 'wb') as fd:
            while not eof:
                    data = peer_data_sock.recv(1024*1024)
                    if not data:
                            eof = True
                    else:
                            fd.write(data)
                            nbytes += len(data)
        peer_data_sock.close()
        t1 = time.time()
        msg = ""
        try:
            msg = ctl_str.recv()
            words = msg.split()
            assert words[0] == "EOF"
            count = int(words[1])
        except:
            return False, "Can not parse EOF message: [%s]" % (msg,) 

        if nbytes != count:
            return False, "Incorrect byte count: EOF message: %d, actual count: %d" % (count, nbytes)

        try:    sndr = gethostbyaddr(addr[0])[0]
        except: sndr = addr[0]
        rate = ''
        size = float(size)/1024.0/1024.0
        if t1 > t0 and size >= 0:
                rate = ' at %f MB/sec' % (size/(t1-t0))
        return True,'Reveived %f MB from %s%s' % (size, sndr, rate)

