/******************************************************************************
 * **                              FENATE                                    **  
 * **          Copyright © 2021, Battelle Memorial Institute                 **
 * **                                                                        **
 * ** 1. Battelle Memorial Institute (hereinafter Battelle) hereby grants    **
 * **  permission to any person or entity lawfully obtaining a copy of this  **
 * **  software and associated documentation files (hereinafter              **
 * **  “the Software”) to redistribute and use the Software in source and    **
 * **  binary forms, with or without modification.  Such person or entity may**
 * **  use, copy, modify, merge, publish, distribute, sublicense, and/or sell**
 * **  copies of the Software, and may permit others to do so, subject to the**
 * **  following conditions:                                                 **
 * **  • Redistributions of source code must retain the above copyright      **
 * **    notice, this list of conditions and the following disclaimers.      **
 * **  • Redistributions in binary form must reproduce the above copyright   **
 * **    notice, this list of conditions and the following disclaimer in     **
 * **    the documentation and/or other materials provided with the          **
 * **    distribution.                                                       **
 * **  • Other than as used herein, neither the name Battelle Memorial       **
 * **    Institute or Battelle may be used in any form whatsoever without    **
 * **    the express written consent of Battelle.                            **
 * ** 2. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS **
 * **  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT     **
 * **  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS     **
 * **  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL BATTELLE   **
 * **  OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,       **
 * **  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT      **
 * **  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, **
 * **  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON     **
 * **  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR    **
 * **  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF    **
 * **  THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF       **
 * **  SUCH DAMAGE.                                                          **
 * ***************************************************************************/

// Modified from source found on
// https://baoilleach.blogspot.com/2019/02/transitive-reduction-of-large-graphs.html
#include <iostream>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>

#include <vector>
#include <iterator>
#include <iostream>
#include <typeinfo>
#include <mpi.h>
#include <string>

#define SAFE_CALLOC(ptr, elem, type) {ptr = (type *) calloc(elem, sizeof(type)); if(!ptr){std::cerr << "Memory allocation failure" << std::endl;}}

#define SAFE_MALLOC(ptr, type, size) {ptr = (type *) malloc(size); if(!ptr){std::cerr << "Memory allocation failure" << std::endl;}}

template <typename Iterator>
std::vector<std::pair<Iterator, Iterator>> break_ranges(Iterator begin, Iterator end, std::size_t n){
   auto sz = std::distance(begin, end);
   auto group = sz / n;
   auto rem   = sz % n;
   std::vector<std::pair<Iterator, Iterator>> ranges;
   ranges.reserve(n);
   for (long int i = 0; i < n -1; ++i){
      auto next_end = std::next(begin, group + (rem ? 1 : 0));
      ranges.emplace_back(begin, next_end);
      begin = next_end;
      if(rem) --rem;
   }
   ranges.emplace_back(begin, end);
   return ranges;
}

using namespace boost;

typedef adjacency_list<listS, vecS, directedS> Graph;
typedef typename graph_traits<Graph>::vertex_descriptor Vertex;
typedef typename graph_traits<Graph>::edge_descriptor Edge;

class DFS
{
public:
  DFS(Graph &graph): m_graph(graph), SEEN(0), CHILD(0)
  {
    SAFE_CALLOC(visited, num_vertices(m_graph), long);
    SAFE_CALLOC(children, num_vertices(m_graph), long);
    child_edges = new Edge[num_vertices(m_graph)];
  }
  ~DFS()
  {
    free(visited);
    free(children);
    delete child_edges;
  }
  void visit(Vertex root)
  {
    SEEN++;
    dfs_visit(root);
  }
  void setParent(Vertex parent)
  {
    CHILD++;

    typename graph_traits<Graph>::out_edge_iterator ei, ei_end;
    for (tie(ei, ei_end) = out_edges(parent, m_graph); ei != ei_end; ++ei) {
      Edge edge = *ei;
      Vertex idx = source(edge, m_graph);
      idx = (idx == parent) ? target(edge, m_graph) : idx;
      children[idx] = CHILD;
      child_edges[idx] = edge;
    }

  }

private:
  void dfs_visit(Vertex v)
  {
    visited[v] = SEEN;
    if (children[v] == CHILD)
      remove_edge(child_edges[v], m_graph);

    std::pair<graph_traits<Graph>::adjacency_iterator, graph_traits<Graph>::adjacency_iterator> adjacent_vrt_it;
    for (adjacent_vrt_it = adjacent_vertices(v, m_graph); adjacent_vrt_it.first != adjacent_vrt_it.second; ++adjacent_vrt_it.first) {
      Vertex y = *(adjacent_vrt_it.first);
      if (visited[y] != SEEN)
        dfs_visit(y);
    }
  }

  Graph &m_graph;
  long *visited;
  long SEEN;
  long *children;
  long CHILD;
  Edge *child_edges;
};

void help()
{
  std::cout << "Usage: reduceMPI input.graph output.dot\n"
            << "\n"
            << "Where the input.graph is a DAG described with the following format:\n"
            << "   NUM_VERTICES\n"
            << "   SRC_1 DST_1\n"
            << "   SRC_2 DST_2\n"
            << "   ...\n"
            << "\nVertices are assumed to be numbered from 0 to NUM_VERTICES-1\n";
  exit(1);
}

int main(int argc, char** argv)
{
  if (argc != 3)
    help();

  unsigned VERTEX_COUNT;
  char fout[2048];
  std::string fileout;
  char *str = NULL;
  int rank = -1;
  int processes = -1;
  double start, end;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &processes);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if(rank == 0) std::cout << "Reading...\n";
  start = MPI_Wtime();
  std::ifstream inputfile(argv[1]);
  if (!inputfile) {
    std::cout << "ERROR: Cannot open " << argv[1] << " for reading\n";
    exit(1);
  }

  inputfile >> VERTEX_COUNT;
  Graph graph(VERTEX_COUNT);
  int a, b;
  while (inputfile >> a >> b)
    add_edge(a, b, graph);

  DFS dfs(graph);
  end = MPI_Wtime();
  
  std::cout << "[" << rank << "] :" << "Reading and creating the graph: " << end - start << " seconds " << std::endl;

  typename graph_traits<Graph>::adjacency_iterator ai, ai_end, bi, bi_end;
  typename graph_traits<Graph>::vertex_iterator vi, vi_end, pvi, pvi_end;

  tie(pvi, pvi_end) = vertices(graph);
  auto ranges = break_ranges(pvi, pvi_end, processes);

  if(rank == 0) std::cout << "Reducing...\n";
  
  start = MPI_Wtime();
  auto tmp = ranges[rank];
  vi = tmp.first;
  vi_end = tmp.second;
  for( ; vi != vi_end ; vi++){
    Vertex x = *vi;
    if (x % 100 == 0)
      std::cout << "[" << rank << "]" << x << "\n";
    dfs.setParent(x);
    for (tie(ai, ai_end) = adjacent_vertices(x, graph); ai != ai_end; ++ai) {
      Vertex y = *ai;
      for (tie(bi, bi_end) = adjacent_vertices(y, graph); bi != bi_end; ++bi) {
        Vertex z = *bi;
        dfs.visit(z);
      }
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  end = MPI_Wtime();
  
  std::cout << "[" << rank << "] :" << "Reducing and waiting for everyone: " << end - start << " seconds " << std::endl;


  if(rank == 0) std::cout << "Writing...\n";
  start = MPI_Wtime();
  fileout = to_string(rank);
  str = argv[2];
  
  strncpy(fout, str, 1024);
  strncat(fout, "_", 1024);
  strncat(fout, fileout.c_str(), 1024);
  
  std::ofstream outputfile(fout);
  if (!outputfile) {
    std::cout << "ERROR: Cannot open " << argv[2] << " for writing\n";
    exit(1);
  }

  outputfile << "digraph G {" << std::endl;
  for(auto ptr = tmp.first; ptr != tmp.second; ++ptr){
     Vertex x = *ptr;
      outputfile<< x << ";" << std::endl;
  }
  for(auto ptr = tmp.first; ptr != tmp.second; ++ptr){
     Vertex x = *ptr;
     for(tie(ai, ai_end) = adjacent_vertices(x, graph); ai != ai_end; ++ai){
        Vertex y = *ai;
        outputfile << x << "->" <<  y << " ;" << std::endl;
     }
  }
  outputfile << "}" << std::endl;
  outputfile.close();
  end = MPI_Wtime();

  std::cout << "[" << rank << "] :" << "Writing the files: " << end - start << " seconds " << std::endl;

  MPI_Finalize();
  return 0;
}
