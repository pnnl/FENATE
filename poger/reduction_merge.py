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
import argparse
import re
import json

parser = argparse.ArgumentParser()
parser.add_argument('--dir', type=str, help="Directory to be read")
parser.add_argument('--out', type=str, help="Destinatino of combined directory")

args = parser.parse_args()
directory = args.dir
out = args.out

print ("DIR", directory)
print ("OUT", out)

def readDir(home):
   lof = []
   for root, subdirs, filenames in os.walk(home):
      for fname in filenames:
         fpath = str(root) + "/" + str(fname)
         if(re.search("^output_", fname)):
            lof.append(fpath)
   return lof

def parseContents(lof):
   dc = {}
   for f in lof:
      #print ("FILE f:", f)
      with open(f) as reader:
         lif = reader.readlines()
         for l in lif:
            l = re.sub(";$", "", l)
            l = str(l).strip()
            if(re.search("->", l)):
               #print ("Line", l)
               prts = str(l).split("->")
               if prts[0] in dc:
                  dc[prts[0]].append(prts[1])
               else:
                  dc[prts[0]] = [prts[1]]     
               #print ("PRTS ", prts)
   return dc

def compareDicts(d1, d2):
   k1 = sorted(d1.keys())
   k2 = sorted(d2.keys())
   #print ("Sizes of dictionaries ", len(k1), len(k2))
   if(len(k1) != len(k2)):
      return False
   for k in k1:
      if k in d2:
         #print ("dicts " + str(k) + " " + str(d1[k]) + " " + str(d2[k]))
         if d1[k] == d2[k]:            
            pass
         else:
            return False
      else:
         return False
   return True

def dumpDict(d1, filename):
   k = sorted(d1.keys())
   nk = [int(i) for i in k]
   mval = max(nk)
   with open(filename, "w") as writer:
      writer.write("digraph G {\n")
      for k in range(mval+1):
         writer.write(str(k) + ";\n")
      for k, l in sorted(d1.items(), key=lambda item: int(item[0])):
         for v in l:
            writer.write(str(k) +  "->" + str(v) + " ;\n")

      writer.write("}\n")
      

lof = readDir(directory)
#print ("LOF ", lof )
combined_dic = parseContents(lof)
#golden_dic = parseContents([out])
#fl = compareDicts(combined_dic, golden_dic)
#if (fl == True):
#   print ("Dictionaries are equal")
#   dumpDict(combined_dic, "combined.txt")
#else:
#   print ("Dictionaries differ")
dumpDict(combined_dic, out)
