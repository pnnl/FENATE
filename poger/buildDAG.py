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

import os
from collections import Counter
from itertools import combinations
from itertools import product

def parse_line(line):
    entries = list(map(int,line.split(',')[:-1]))
    sender = entries[1]
    recipient = entries[2]
    message = entries[4]
    send_time = entries[5]
    recv_time = entries[6]
    return (sender, recipient, message, send_time, recv_time)


def build_rankDAG(dirs, r):
    # Because all messages are number sequentially by sender, we need to create a common index for message
    # For messages sent to or from rank r, this builds an index (by data directory) which translates from
    # The ordering from sequential relative to rank, to sequential relative to communication pair
    index = {dir : {} for dir in dirs}
    for dir in dirs:
        messages = {}
        try:
            with open(dir + "/trace_MPISend_" + repr(r) + ".ct", "r") as input:
                for line in input:
                    (send,recv,midx,send_t,recv_t) = parse_line(line)
                    if not (send,recv) in messages:
                        messages[(send,recv)] = [midx]
                    else:
                        messages[(send,recv)].append(midx)
        except:
            pass
        try:
            with open(dir + "/trace_MPIRecv_" + repr(r) + ".ct","r") as input:
                sending_ranks = set([parse_line(line)[0] for line in input])
            for sender in sending_ranks:
                with open(dir +"/trace_MPISend_" + repr(sender) + ".ct","r") as input:
                    messages[(sender,r)] = [parse_line(line)[2] for line in input if parse_line(line)[1] == r]
        except:
            pass
    
        for m in messages:
            index[dir][m] = {idx : i for (i,idx) in enumerate(messages[m])}

    
    
    print("Built Message index")
    print(set([m for loc in index for m in index[loc]]))
    # Build a list of tuples (timestamp, message_info)
    # All timestamps are relative to the MPI rank
    # Embedding builds a per message_info embedding of the timestamps into Z^d where d is the number of directors
    # If embedding[m] < embedding[k] (as elements of the canonical poset on Z^d) then message m always preceeeds message k
    embedding = {}
    for dir in dirs:
        print(dir)
        messages = []
        count = 0
        try:
            with open(dir + "/trace_MPIAllreduce_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("AllReduceStart",recv,midx)))
                    messages.append((post_t,("AllReduceEnd",recv,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("AllReduce Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIGather_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("GatherStart",recv,midx)))
                    messages.append((post_t,("GatherEnd",recv,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Gather Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIReduce_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("ReduceStart",recv,midx)))
                    messages.append((post_t,("ReduceEnd",recv,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Reduce Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIScatter_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("ScatterStart",send,midx)))
                    messages.append((post_t,("ScatterEnd",send,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Scatter Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIAlltoall_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("AlltoallStart",-1,midx)))
                    messages.append((post_t,("AlltoallEnd",-1,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Alltoall Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIBcast_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("BcastStart",send,midx)))
                    messages.append((post_t,("BcastEnd",send,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Bcast Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPISendrecv_" + repr(r) + ".ct",'r') as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,("Sendrecv_"+repr(send)+"Start",recv,midx)))
                    messages.append((post_t,("Sendrecv_" + repr(send)+ "End",recv,midx)))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Sendrecv Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPISend_" + repr(r) + ".ct", "r") as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((prior_t,(send,recv,index[dir][(send,recv)][midx])))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Send Done", count, len(messages))
        count = 0
        try:
            with open(dir + "/trace_MPIRecv_" + repr(r) + ".ct","r") as input:
                for line in input:
                    #print(line)
                    count += 1
                    (send,recv,midx,prior_t,post_t) = parse_line(line)
                    messages.append((post_t,(send,recv,index[dir][(send,recv)][midx])))
        except Exception as E:
            print(E.__class__.__name__,E)
        print("Recieved Done", count, len(messages))
        messages.sort()
        messages = [m for (t,m) in messages]
        for (i,m) in enumerate(messages):
            if not m in embedding:
                embedding[m] = [i]
            else:
                embedding[m].append(i)
    print("Total Messages", len(embedding))

    # This is the list of directed temporally (m,k) in edges implies embedding[m] < embedding[k]
    edges = []

    send = [m for m in embedding if m[0] == r]
    recieve = [m for m in embedding if m[1] == r and not isinstance(m[0],str)]
    collectives = [m for m in embedding if isinstance(m[0],str)]
    collective_types = [m[0][:-3] for m in embedding if isinstance(m[0],str) and m[0][-3:] == "End"]
    
    # Ordering the Sends
    # Need to figure out how many times rank r sends to v 
    count = {}
    for (u,v,idx) in send:
        count[v] = max(count.get(v,-1),idx)
    for v in count:
        for j in range(count[v]):
            edges.append(((r,v,j),(r,v,j+1)))

        
    #Note that the edges  (r,"AllReduceStart", j) -> (r, "AllReduceStart",j+1) is achieved by the send process 
    #allreduce = max([m[2] for m in embedding if m[1] == "AllReduceStart"], default = -1)
    #edges.extend([((r,"AllReduceStart",j),(r,"AllReduceEnd",j)) for j in range(allreduce)])
    #edges.extend([((r,"AllReduceEnd",j),(r,"AllReduceStart",j+1)) for j in range(allreduce-1)])

    # Edges from recieve to send
    #print(send)
    #print(recieve)
    for (m,k) in product(recieve, send):
        if all( a < b for (a,b) in zip(embedding[m],embedding[k])):
            edges.append((m,k))

    for (m,k) in product(recieve, [ell for ell in collectives if ell[0][-5:] == "Start"]):
        if all( a < b for (a,b) in zip(embedding[m],embedding[k])):
            edges.append((m,k))
            
    for (m,k) in product([ell for ell in collectives if ell[0][-3:] == "End"],send):
        if all( a < b for (a,b) in zip(embedding[m],embedding[k])):
            edges.append((m,k))
    for (m,k) in product(collectives,collectives):
        if all(a < b for (a,b) in zip(embedding[m],embedding[k])):
            edges.append((m,k))

    return edges
    


if __name__ == '__main__':
    import argparse
    import os
    import time
    import networkx as nx
    
    parser = argparse.ArgumentParser(description = 'Build a DAG from MPI collection of MPI traces')
    parser.add_argument('rootdir',  help='Directory containing multiple MPI runs of with same message set')
    parser.add_argument('--rank',  default = argparse.SUPPRESS, type = int, help ='Indicates the rank that is currently being processed.  If not present, this assumes that all rank covers have been produced and this will build the final DAG')
    parser.add_argument('--dest', default = '/MPICovers')
    parser.add_argument('--reduce', default = '/reduction')

    args = vars(parser.parse_args())
    print(args)
    

    root = args['rootdir'].rstrip('/') + '/'
    print(root)
    dirs = os.listdir(root)
    dirs = [d for d in dirs if os.path.isdir(root + d)]
    dirs = [d for d in dirs if not d == args['dest'].rstrip('/')]
    dirs = [root +d for d in dirs]
    if 'rank' not in args:
        print('Building DAG from individual ranks')
        if not os.path.isdir(root + args['dest']):
            print('Destination directory', args['dest'], 'does not exist')
            raise Exception('UnknownDestination')
        mapping = set([])
        E = set([])
        count = 0
        for file in os.listdir(root + args['dest']):
            # Check if it is the right type of file.
            if len(file) < 11:
                continue
            if not file[-4:] == '.txt':
                continue
            if not file[:11] == 'covers_rank':
                continue
            rank = file.split('.')[0][11:]
            with open(root + args['dest'] + '/' + file,'r') as input:
                rankE = [tuple(line.rstrip().split('-->')) for line in input]

            for (u,v) in rankE:
                if not u in mapping:
                    mapping.add(u)
                if not v in mapping:
                    mapping.add(v)
            E.update(rankE)

            print("Rank", rank)
            print("\t Edges", len(rankE))
            print("\t Total Edges", len(E))
            print("\t Total Messages", len(mapping))
        print("Completed Building DAG")

        print(mapping)
        mapping = [(m.lstrip('(').split(',')[:2], int(m.rstrip(')').split(',')[-1]), m) for m in mapping]
        mapping.sort(reverse = True)
        mapping = [m for (pair, idx, m) in mapping]
        print(mapping)
        with open(root + args['dest'] + '/messages.txt','w') as output:
            for v in mapping:
                output.write(v + '\n')
        mapping = {m : repr(i) for (i,m) in enumerate(mapping)}
        with open(root + args['dest'] + '/DAG.txt','w') as output:
            output.write(repr(len(mapping)) + "\n")
            for (u,v) in E:
                output.write(mapping[u] + ' ' + mapping[v] + '\n')
        
            
            
    elif args['rank'] == 0:
        print('Creating directory if needed')
        if not os.path.isdir(root + args['dest']):
            os.mkdir(root + args['dest'])
        if not os.path.isdir(root + args['dest']  + args['reduce']):
            os.mkdir(root + args['dest'] + args['reduce'])
            
        print('Building rank 0 covers')
        E = build_rankDAG(dirs,0)
        V = list(set([u for (u,v) in E] + [v for (u,v) in E]))
        with open(root + args['dest'] + '/messages_rank0.txt','w') as output:
            for v in V:
                output.write(repr(v) + "\n")
        V = { v : repr(i) for (i,v) in enumerate(V)}
        
        with open(root + args['dest'] + '/DAG_rank0.txt','w') as output:
            output.write(repr(len(V)) + '\n')
            for (u,v) in E:
                output.write(V[u] + " " + V[v] + "\n")
    else:
        print('Building rank', args['rank'], 'covers')
        # Wait 2 minutes for the directory to be created
        for _ in range(120):
            if os.path.isdir(root + args['dest']):
                break
            else:
                time.sleep(1)
        else:
            # Directory doesn't exist and not created
            print('Destination directory', args['dest'], 'does not exist and was not created')
            raise Exception('UnknownDestination')
        E = build_rankDAG(dirs,args['rank'])
        V = list(set([u for (u,v) in E] + [v for (u,v) in E]))
        with open(root + args['dest'] + '/messages_rank' + repr(args['rank']) + '.txt','w') as output:
            for v in V:
                output.write(repr(v) + "\n")
        V = { v : repr(i) for (i,v) in enumerate(V)}
        
        with open(root + args['dest'] + '/DAG_rank' + repr(args['rank']) +'.txt','w') as output:
            output.write(repr(len(V)) + '\n')
            for (u,v) in E:
                output.write(V[u] + " " + V[v] + "\n")

            
     
    
            
        
    
    
 
            
