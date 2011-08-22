#--
#@(#)stats.py
#
# stats
# Copyright(c) 2011 Supreet Sethi <supreet.sethi@gmail.com> 

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import task
from recieve_pb2 import Event
from logging import basicConfig, info, debug, warning
import logging
from time import time
from bisect import bisect
from socket import socket, gethostbyaddr
from optparse import OptionParser
from hashlib import sha1
logging_args = {
         'format': '%(asctime)s %(module)s %(levelname)s %(message)s',
         'level': logging.INFO,
    }


basicConfig(**logging_args)


class CarbonUnreachable(Exception):
    pass

class NonProtocolException(Exception):
    pass

def NonProtocalHandler(msg):
    """ Takes in legacy message type and converts into protobuf"""
    event = Event()
    name, rest = msg.split(':')
    event.name = name
    val, rest = rest.split('|')
    event.value = int(val)
    if len(rest) == 1:
        _type = rest
        
    else:
        _type, rate = rest.split('@')
        event.sample_rate = float(rate)
    if _type != 'c' and _type != 's':
        raise(NonProtocolException())
    event.event_type = 0 if _type == 'c' else 1
    return event


class StatsDatagramProtocol(DatagramProtocol):
    def __init__(self, graphiteHost, graphitePort):
        self.counters = {}
        self.timers = {}
        self.graphiteHost = graphiteHost
        self.graphitePort = graphitePort
        
    def counter(self, message, host):
        l_key  = ('%s.%s' % (host, message.name), 
                             'stats.%s' % message.name)
        for key in l_key: 
            if self.counters.has_key(key):
                self.counters[key] += message.value
            else: 
                self.counters[key] = message.value
        return

        
    def timer(self, message, host):
        l_key  = ('%s.%s' % (host, message.name), 
                             'stats.samples.%s' % message.name)
        for key in l_key: 
            if self.timers.has_key(key):
                self.timers[key].append( message.value )
            else: 
                self.timers[key] = [message.value]
        return


    def datagramReceived(self, datagram, host):
        if datagram.startswith('\n'):
            info("Treating message from %s as protobuf" % host)
            try:
                msg = Event.FromString(datagram)
            except Exception, msg_e:
                warning("The message not compatible %s", str(msg_e))
        else:
            try:
                msg = NonProtocalHandler(datagram)
            except Exception, nph_e:
                warning("unparsable message from %s", str(nph_e))
        try:
            ht = gethostbyaddr(host[0])[0]
        except:
            ht = sha1(host[0]).hexdigest[:10]
        if msg.event_type == 0:
            self.counter(msg, ht)
        else:
            self.timer(msg, ht)
        return


    def pushToGraphite(self):
        info("Graphite push")
        buf = []
        msg_cnt = 0
        ts = int(time())
        for counter, val in self.counters.items():
            buf.append('%s %d %d' % (counter, val, ts))
            msg_cnt += 1
        for samples, val in self.timers.items():
            if len(val) != 0:
                val.sort()
                _min = val[0]
                _max = val[-1]
                avg = int(sum(val)/ len(val))
                
                buf.append('%s.lower %d %d' % (samples, _min, ts))
                buf.append('%s.upper %d %d' % (samples, _max, ts))
                buf.append('%s.count %d %d' % (samples, len(val), ts))
                buf.append('%s.mean %d %d' % (samples, avg, ts))
                msg_cnt +=1
        self.timers = {}
        buf.append('statsd.numStats %d %d' % (msg_cnt, ts))
        _res = '\n'.join(buf)
        sock = socket()
        try:
            sock.connect((self.graphiteHost, self.graphitePort))
        except Exeption, sock_e:
            warning("Cannot access carbon server %s", str(e))
            raise CarbonUnreachable()
        sock.sendall(_res)
        sock.close()
        return




def main(ch, cp, p):
    sdp = StatsDatagramProtocol(ch, cp)
    loop = task.LoopingCall(sdp.pushToGraphite)
    loop.start(10)
    t = reactor.listenUDP(p, sdp)
    info("Started twisted stats on 0.0.0.0:%d" % p)
    reactor.run()
    
    
if __name__ == '__main__':
    parser = OptionParser('usage: %prog [options] ')
    parser.add_option('-c', '--carbon-host', dest='ch', default=None)
    parser.add_option('-r', '--carbon-port', dest='cp', default=2003, 
                      type='int')
    parser.add_option('-f', '--logfile', dest='logfile', default=None)
    parser.add_option('-p','--port', dest='server_port', default=8125, 
                      type='int')
    

    parser.add_option('-v', '--logginglevel', dest='level', default='INFO')
    options, args = parser.parse_args()

    if not hasattr(logging, options.level):
        parser.error("level %s is not acceptable" % options.level)
    logging_args = {
        'format': '%(asctime)s %(module)s %(levelname)s %(message)s',
         'level': getattr(logging, options.level),
         }
    logging.basicConfig(**logging_args)
    import sys
    if options.ch == None:
        sys.exit(-1)
    main(options.ch, options.cp, options.server_port)

