#############################################################################
##                               FENATE                                    ##  
##          Copyright © 2021, Battelle Memorial Institute                  ##
##                                                                         ##
## 1. Battelle Memorial Institute (hereinafter Battelle) hereby grants     ##
##  permission to any person or entity lawfully obtaining a copy of this   ##
##  software and associated documentation files (hereinafter               ##
##  “the Software”) to redistribute and use the Software in source and     ##
##  binary forms, with or without modification.  Such person or entity may ##
##  use, copy, modify, merge, publish, distribute, sublicense, and/or sell ##
##  copies of the Software, and may permit others to do so, subject to the ##
##  following conditions:                                                  ##
##  • Redistributions of source code must retain the above copyright       ##
##    notice, this list of conditions and the following disclaimers.       ##
##  • Redistributions in binary form must reproduce the above copyright    ##
##    notice, this list of conditions and the following disclaimer in      ##
##    the documentation and/or other materials provided with the           ##
##    distribution.                                                        ##
##  • Other than as used herein, neither the name Battelle Memorial        ##
##    Institute or Battelle may be used in any form whatsoever without     ##
##    the express written consent of Battelle.                             ##
## 2. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS  ##
##  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT      ##
##  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS      ##
##  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL BATTELLE    ##
##  OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,        ##
##  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT       ##
##  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,  ##
##  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON      ##
##  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR     ##
##  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF     ##
##  THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF        ##
##  SUCH DAMAGE.                                                           ##
#############################################################################

import simpy
import networkx as nx
from itertools import combinations
from LPS import LPS
from itertools import product
import numpy as np
from simpy.events import AllOf
from simpy.util import start_delayed


def letter_gen():
    from string import ascii_lowercase as letters
    k = 1
    while True:
        yield from product(letters, repeat = k)
        k += 1
        

# Base class for topology
#    env -- discrete event simluator environment the topology lives in
#    name -- name for the topology, used to anotate packet reporting
#    duplex -- if True, links are support simultaneous communication in both direction at full bandwidth
#            -- if False, links support communication in one direction at a time at full bandwidth
#    latency -- vertex specific latency values for eager and RDMA protocol
#    node_latency -- default latency for compute node interaction (eager costs going in and out, RDMA just on send)
#    switch_latency -- default latency for switch traversal 
#    bw -- bandwidth of non-RDMA packets
#    # packet generator is a represnts a packet with the following time:
#    start_time -- time packet starts moving
#    bytes -- number of bytes in packet
#    route -- route packet is taking through network
#    id -- an identifier for packet
#    sync -- if None, a syncronous transmission, otherwise number of bits in the completion packet 
class Topology():
    def __init__(self,env = None, name = None, duplex = True):
        #print(env)
        if env == None:
            self.env = simpy.Environment()
        else:
            self.env = env
        if name == None:
            self.name = "Topology"
        else:
            self.name = name
        self.edges = {}
        self.bw = {'RDMA' : 12.5, 'eager' : 3.5}  # Estimated from BlueSky, 4x InfiniBand EDR
        self.route = None # Place holder for routing function
        self.statistics = {}
        self.node_latency = {'eager': 650, 'RDMA' : 750} 
        self.switch_latency = {'eager' : 100, 'RDMA' : 90}
        self.latency = {} # Records entity specific latencies
        self.duplex = duplex
        self.switch = set([])
        self.packet_limit = 4096  # Assume a 4K packet limit
        self.packet_overhead = 98 # Assume a maximum InfiniBand overhead
        self.eager_limit = 16384  # Assume threshold for eager is 16K
        self.rank2node = None # MPI rank to node name (on full system)
        self.node2rank = None # Canoncial node name to MPI rank (on full system)
        
        #print(env)
        #print("Initialized Topology")

    # hold the resource associated with the request until 
    def transmit(self, resource, request, transmit_time):
        #print("Holding",self.env.now, transmit_time)
        yield self.env.timeout(transmit_time)
        #print("Releasing", self.env.now)
        resource.release(request)

    def packet_id(self,id):
        if id == None:
            while True:
                yield None
        else:
            g = letter_gen()
            while True:
                yield (id, next(g))
                
    def message(self, source,dest, message_size, start_time, id = None, method = None, sync = True, packet_stats = False):
        if start_time == 0:
            self.env.process(self.send(source,dest,message_size, id = id, method = method, sync = sync, packet_stats = packet_stats))
        else:
            start_delayed(self.env, self.send(route,message_size, id = id, method = method, sync = sync, packet_stats = packet_stats), start_time)
        
    def send(self, source, dest, message_size, id = None, method = None, sync = True, packet_stats = False):
        pid = self.packet_id(id)
        # If there is a message id, assume we want statistics
        if not id == None:
            self.statistics[id] = [self.env.now]

        # Automatically determine transferm method if there is none specified
        if method == None:
            if message_size > self.eager_limit:
                method = 'rendezvous'
            else:
                method = 'eager'

        R = self.route(source,dest)
                
        # Transmit using eager method
        if method == 'eager':
            if message_size > self.packet_limit:
                packets = list(zip( [self.packet_limit+self.packet_overhead]*(message_size//self.packet_limit),pid))
                partial = message_size%self.packet_limit
                if not partial == 0:
                    packets.append((partial+self.packet_overhead,next(pid))) 
            else:
                packets = [(self.packet_overhead + message_size,next(pid))]
            yield AllOf(self.env, [self.env.process(self.packet(R,size, id = pkt, stats = packet_stats)) for (size,pkt) in packets])
        elif method == 'rendezvous':
            yield self.env.process(self.packet(R,self.packet_overhead, id = (id,'rndz_start'), stats = packet_stats))
            yield self.env.process(self.packet(R[::-1],self.packet_overhead, id = (id, 'rndz_reply'), stats = packet_stats))
            # RDMA data transfer
            packets = list(zip( [self.packet_limit + self.packet_overhead]*(message_size//self.packet_limit),pid))
            partial = message_size%self.packet_limit
            if not partial == 0:
                packets.append((self.packet_overhead + partial,next(pid)))
            yield AllOf(self.env, [self.env.process(self.packet(R,size, id = pkt, stats = packet_stats, RDMA = True)) for (size,pkt) in packets])
            yield self.env.process(self.packet(R[::-1],self.packet_overhead, id = (id, 'fin'), stats = packet_stats))
            
        if not id == None:
            self.statistics[id].append(self.env.now)
        if sync:
            yield self.env.process(self.packet(R[::-1],self.packet_overhead, id = (id,'sync'), stats = packet_stats))
            if not id == None:
                self.statistics[id].append(self.env.now)
    
    def packet(self,R,packet_size,id = None, stats = False, RDMA = False):
        if stats:
            self.statistics[id] = [(R[0], self.env.now)]

            
        if RDMA:
            mode = 'RDMA'
            packet_time = int(np.ceil((packet_size + self.packet_overhead)/self.bw['RDMA']))
        else:
            packet_time = int(np.ceil((packet_size + self.packet_overhead)/self.bw['eager']))
            mode = 'eager'
        
        for e in zip(R,R[1:]):
            # Latency for e[0]
            if e[0] in self.switch:
                yield self.env.timeout(self.latency.get(e[0],self.switch_latency)[mode])
            else:
                yield self.env.timeout(self.latency.get(e[0],self.node_latency)[mode])

            if stats:
                self.statistics[id].append((e[0], self.env.now))
            # Wait for availbility of edge e
            req = self.edges[e].request()
            yield req
            if stats:
                self.statistics[id].append((e[0], self.env.now))
            # Hold edge e for long enough for everything to be transmitted
            # If e[1] is a switch, then hold time includes time to traverse the switch
            if e[1] in self.switch:
                self.env.process(self.transmit(self.edges[e],req, self.latency.get(e[1],self.switch_latency)[mode] + packet_time))
            else:
                self.env.process(self.transmit(self.edges[e],req, packet_time))

            # If e[1] is a compute node, there is a latency cost for entering the node
            if not RDMA and not e[1] in self.switch:
                yield self.env.timeout(self.latency.get(e[1],self.node_latency)[mode])
            # If we want to add delay for transiting lines it would be added here
            if stats:     
                self.statistics[id].append((e[1], self.env.now))
        

        # The packet isn't done until the last bit arrives
        yield self.env.timeout(packet_time)
        if stats:     
            self.statistics[id].append((e[1], self.env.now))

    def DAGsend(self,source,dest, message_size, resource, block_count, blocked_by, id = None, method = None, sync = True, packet_stats = False):
        if resource == None:
            req = []
        else:
            req = [resource.request(priority = -1) for _ in range(block_count)]
            yield AllOf(self.env,req)

        
        breq = [b.request(priority = 0) for b in blocked_by]
        yield AllOf(self.env,breq)
        yield self.env.process(self.send(source,dest,message_size, id = id, method = method, sync = sync, packet_stats = packet_stats))
        for r in req:
            resource.release(r)
        for b,resource in zip(breq,blocked_by):
            resource.release(b)
          
    
    # Send messages according to a DAG where the vertices are messages
    # Directed edge (i,j) implies that the message i must be completed before message j can be sent
    # Message properties are atributes of node
    # No ability to add wait time at compute nodes
    # Easiest application is to recovered dependencies from MPI traces
    def messageDAG(self, D):
        messages = {v : simpy.PriorityResource(self.env,capacity = D.out_degree(v)) for v in D.nodes if D.out_degree(v) > 0}
        for v in D.nodes:
            if D.out_degree(v) == 0:
                messages[v] = None
        for M in D.nodes:
            source, dest, idx = M
            method = D.nodes[M].get("method",None)
            packet_stats = D.nodes[M].get("packet_stats",False)
            sync = D.nodes[M].get("sync",True)
            message_size = D.nodes[M]["message_size"]
            self.env.process(self.DAGsend(source,dest, message_size, messages[M], D.out_degree(M), [messages[x] for x in D.predecessors(M)], packet_stats = packet_stats, id = M, method = method, sync = sync))

    # Send messages according to a DAG where vertices are copies of compute nodes
    # Messages are given by the directed edges
    # Directed edge (i,j) implies that the message from i must be recieved by j before j can send outbound messages
    # Message sizes are given by the weight of the edge (in bytes)
    # Wait-time at compute node (for computation) is given by weights on nodes (in nanoseconds)
    # Easiest application is for structural compute paradigms like Sweep3D.
    def computeDAG(self, D, packet_stats = False):
        pass



def PingPong(a,b,size, repeats = 10):
    D = nx.DiGraph()
    D.add_edges_from([((a,b,r),(b,a,r)) for r in range(repeats)])
    D.add_edges_from([((b,a,r),(a,b,r+1)) for r in range(repeats-1)])
    nx.set_node_attributes(D,size, "message_size")
    nx.set_node_attributes(D,False,"sync")
    nx.set_node_attributes(D,True,"packet_stats")

    return D

# Message version of sweep3d
# Assumpe mapping takes (x,y) -> node, i.e. mapping[(x,y)] = node
def Sweep3D(x,y,size,mapping = None):
    if mapping == None:
        mapping = {(i,j) : y*i + j for i in range(x) for j in range(y)}
    elif not len(mapping) ==x*y:
        print("Missized mapping, using standard mapping")
    edges = []
    for i in range(x):
        for j in range(y):
            if i+2 < x:
                edges.append(((mapping[(i,j)],mapping[(i+1,j)],0),(mapping[(i+1,j)],mapping[(i+2,j)],0)))
            if j+2 < y:
                edges.append(((mapping[(i,j)],mapping[(i,j+1)],0),(mapping[(i,j+1)],mapping[(i,j+2)],0)))
            if i+1 < x and j+1 < y:
                edges.append(((mapping[(i,j)],mapping[(i+1,j)],0),(mapping[(i+1,j)],mapping[(i+1,j+1)],0)))
                edges.append(((mapping[(i,j)],mapping[(i,j+1)],0),(mapping[(i,j+1)],mapping[(i+1,j+1)],0)))
    D = nx.DiGraph()
    D.add_edges_from(edges)
    nx.set_node_attributes(D,size, "message_size")
    nx.set_node_attributes(D,False,"sync")
    nx.set_node_attributes(D,True,"packet_stats")

    return D
    
        

        
