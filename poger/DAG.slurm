#!/bin/bash

#SBATCH --nodes=32
#SBATCH --time=48:00:00
#SBATCH --exclusive

#DEST="/MPICovers"
#REDUCE="/reduction"


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

module purge
module load python/anaconda3.2019.3
source /share/apps/python/anaconda3.2019.3/etc/profile.d/conda.sh
module load boost/1.68
module load gcc/7.1.0
module load openmpi/2.1.1

echo ${HOME}${FOLDER}
echo $MAXRANK

time -p (
for rank in $(seq 0 $MAXRANK);
do
    #echo "srun --nodes=1 --ntasks=1 --exclusive python buildDAG.py ${HOME}$FOLDER --rank $rank --dest $DEST --reduce $REDUCE &"
    srun --nodes=1 --ntasks=1 --exclusive  --output=${HOME}${FOLDER}${DEST}/build_${rank}_output.txt python buildDAG.py ${HOME}$FOLDER --rank $rank --dest $DEST --reduce $REDUCE &
    if !(($rank % 100)); then
	sleep 10
    fi
done

wait
)
echo "Built comparabilities for all ranks."

time -p (
for rank in $(seq 0 $MAXRANK);
do
    srun --nodes=1 --ntasks=1 --exclusive --output=${HOME}${FOLDER}${DEST}/reduce_${rank}_output.txt ./reduce ${HOME}${FOLDER}${DEST}/DAG_rank${rank}.txt ${HOME}${FOLDER}${DEST}/reducedDAG_rank${rank}.dot  &
    if !(($rank % 100)); then
	sleep 10
    fi
done

wait
)
echo "Finished rank-by-rank transitive reduction"

time -p (
for rank in $(seq 0 $MAXRANK);
do
    srun --nodes=1 --ntasks=1 --exclusive python covers.py ${HOME}${FOLDER}${DEST}/reducedDAG_rank${rank}.dot ${HOME}${FOLDER}${DEST}/messages_rank${rank}.txt ${HOME}${FOLDER}${DEST}/covers_rank${rank}.txt &
    if !(($rank %100)); then
	sleep 10
    fi
done

wait
)
echo "Converted all ranks back to message format"

time -p srun --nodes=1 --ntasks=1 --exclusive python buildDAG.py ${HOME}$FOLDER --dest $DEST

echo "Built total DAG"

time -p mpirun -np 640 --map-by core --mca mpi_cuda_support 0 ./reduceMPI ${HOME}${FOLDER}${DEST}/DAG.txt ${HOME}${FOLDER}${DEST}$REDUCE/output &> ${HOME}${FOLDER}${DEST}/reduce_log.txt

echo "Completed MPI transitive reduction on entire DAG"

time -p srun --nodes=1 --ntasks=1 --exclusive python reduction_merge.py --dir=${HOME}${FOLDER}${DEST}${REDUCE} --out=${HOME}${FOLDER}${DEST}/reducedDAG.dot

echo "Completed merge of transitive reduction files"

time -p srun --nodes=1 --ntasks=1 --exclusive python covers.py ${HOME}${FOLDER}${DEST}/reducedDAG.dot ${HOME}${FOLDER}${DEST}/messages.txt ${HOME}${FOLDER}${DEST}/covers.txt 

echo "Done."


