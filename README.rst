cgraph module
=============

The cgraph module is a high performance Directed Acyclic Graph implemented
in Cython. Expect graph traversals to be several hundred to thousands of
times faster than Networkx for large graphs. Graph's with several hundred 
thousand nodes are handled with aplomb. That said, the cgraph module
doesn't have near the features of Networkx. The module is designed solely
for efficiently building, traversing, reversing, and introspecting 
large graphs. A graph theory library it is not. Most of the graph methods
return iterators so that large graphs may be efficiently managed by user code.

