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

import sys

if __name__ == '__main__':
    dot_graph = sys.argv[1]
    messages = sys.argv[2]
    outfile = sys.argv[3]

    print("DOT Graph:", dot_graph)
    print("Message Lst:", messages)
    print("Output File:", outfile)

    with open(messages,'r') as input:
        M = [line.rstrip() for line in input]

    with open(outfile,'w') as output:
        with open(dot_graph,'r') as graph:
            for line in graph:
                line = line.rstrip() # remove any whitespace including newline
                # Check to make sure it is a node or edge line
                if not line[-1] == ';':
                    continue
                line = line.rstrip(';').rstrip() # Remove ';' and whitespace
                nodes = line.split('->')
                if not len(nodes) == 2:
                    #print(line)
                    continue
                (u,v) = list(map(int,nodes))
                output.write(M[u] + '-->' + M[v] + '\n')
        
            
