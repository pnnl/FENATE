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

using namespace boost;

typedef adjacency_list<listS, vecS, directedS> Graph;
typedef typename graph_traits<Graph>::vertex_descriptor Vertex;
typedef typename graph_traits<Graph>::edge_descriptor Edge;

class DFS
{
public:
  DFS(Graph &graph): m_graph(graph)
  {
    seen = (long*) calloc(num_vertices(m_graph), sizeof(long)); // sets to 0
    children = (long*) calloc(num_vertices(m_graph), sizeof(long)); // sets to 0
    child_edges = (Edge*) malloc(num_vertices(m_graph) * sizeof(Edge));
  }
  ~DFS()
  {
    free(seen);
    free(children);
    free(child_edges);
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
      Vertex src = source(edge, m_graph);
      src = (src == parent) ? target(edge, m_graph): src;
      children[src] = CHILD;
      child_edges[src] = edge;
    }

  }

private:
  void dfs_visit(Vertex v)
  {
    seen[v] = SEEN;
    if (children[v] == CHILD)
      remove_edge(child_edges[v], m_graph);

    std::pair<graph_traits<Graph>::adjacency_iterator, graph_traits<Graph>::adjacency_iterator> adj_vertices_it;
    for (adj_vertices_it = adjacent_vertices(v, m_graph); adj_vertices_it.first != adj_vertices_it.second; ++adj_vertices_it.first) {
      Vertex x = *(adj_vertices_it.first);
      if (seen[x] != SEEN)
        dfs_visit(x);
    }
  }

  Graph &m_graph;
  long *seen;
  long SEEN = 0; // flag used to mark visited vertices
  long *children;
  long CHILD = 0; // flag used to mark children of parent
  Edge *child_edges;
};

void help()
{
  std::cout << "Usage: reduce input.graph output.dot\n"
            << "\n"
            << "Where the input.graph is a DAG described with the following format:\n"
            << "   NUM_VERTICES\n"
            << "   SRC_1 DST_1\n"
            << "   SRC_2 DST_2\n"
            << "   ...\n"
            << "\nVertices are assumed to be numbered from 0 to NUM_VERTICES-1\n";
  exit(1);
}

int main(int argc, char* argv[])
{
  if (argc != 3)
    help();

  unsigned VERTEX_COUNT;

  std::cout << "Reading...\n";
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

  typename graph_traits<Graph>::adjacency_iterator ai, ai_end, bi, bi_end;
  typename graph_traits<Graph>::vertex_iterator vi, vi_end;

  std::cout << "Reducing...\n";
  for (tie(vi, vi_end) = vertices(graph); vi != vi_end; ++vi) {
    Vertex x = *vi;
    if (x % 100 == 0)
      std::cout << x << "\n";
    dfs.setParent(x);
    for (tie(ai, ai_end) = adjacent_vertices(x, graph); ai != ai_end; ++ai) {
      Vertex y = *ai;
      for (tie(bi, bi_end) = adjacent_vertices(y, graph); bi != bi_end; ++bi) {
        Vertex z = *bi;
        dfs.visit(z);
      }
    }
  }

  std::cout << "Writing...\n";
  std::ofstream outputfile(argv[2]);
  if (!outputfile) {
    std::cout << "ERROR: Cannot open " << argv[2] << " for writing\n";
    exit(1);
  }
  write_graphviz(outputfile, graph);

  return 0;
}
