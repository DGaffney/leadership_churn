#!/usr/bin/python
#only works with python 2.6+
#usage python scripts/stats.py [infile_name] [outfile_name] [directed(true|false)]
import sys
import igraph
import csv
directed = sys.argv[3] == "true"
g = igraph.Graph.Read_Ncol(sys.argv[1],directed=directed)
vertices = []
for v in g.vs:
  vertices.append(v['name'])
  
communities = []
for c in g.community_infomap():
  communities.append(c)

with open(sys.argv[2], 'w') as csvfile:
  writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
  writer.writerow(["vertices", vertices])
#  writer.writerow(["betweenness", g.betweenness(vertices=None, directed=directed, cutoff=None)])
#  writer.writerow(["clustering_coeff", g.transitivity_local_undirected(vertices=None, mode="nan", weights=None)])
#  writer.writerow(["pagerank", g.personalized_pagerank(vertices=None, directed=directed)])
  writer.writerow(["degree_distribution", g.degree()])
  writer.writerow(["indegree_distribution", g.indegree()])
  writer.writerow(["outdegree_distribution", g.outdegree()])
#  writer.writerow(["eccentricity", g.eccentricity(mode=igraph.IN if directed else igraph.ALL)])
#  writer.writerow(["eigenvector_centrality", g.eigenvector_centrality(directed=directed)])
#  writer.writerow(["coreness", g.coreness(mode=igraph.IN if directed else igraph.ALL)])
#  writer.writerow(["authority_scores", g.authority_score()])

