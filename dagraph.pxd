

#------------------------------------------------------------------------------
# Node Type
#------------------------------------------------------------------------------

cdef class DAGNode:

    cdef object content        # The user's python object for a node
    cdef list parents          # The parent DAGNodes of this DAGNode
    cdef list children         # The child DAGNodes of this DAGNode
    cdef bint _check_parents   # Ensure no duplicate parents
    cdef bint _check_children  # Ensure no duplicate children

    cdef inline bint has_children(self)
    cdef inline bint has_parents(self)
    cdef inline void check_children(self, bint)
    cdef inline void check_parents(self, bint)

    cdef void add_parent(self, DAGNode)
    cdef void add_child(self, DAGNode)
    cdef void remove_parent(self, DAGNode)
    cdef void remove_child(self, DAGNode)


#------------------------------------------------------------------------------
# Graph Type
#------------------------------------------------------------------------------

cdef class DAGraph(object):
    
    cdef dict _graph_nodes   # Maps user objects -> DAGNodes
    cdef bint _cycle_detect  # Perform cycle detection when adding an edge

    cdef inline bint contains_fast(self, object)
    cdef inline DAGNode get_node_fast(self, object)
    cdef inline cycle_detect_fast(self, bint)
    
    cdef add_node_fast(self, object)
    cdef delete_node_fast(self, object)
    
    cdef add_edge_fast(self, object, object)
    cdef add_edges_parents_fast(self, tuple, object)
    cdef add_edges_children_fast(self, object, tuple)
    cdef remove_edge_fast(self, object, object)
    cdef remove_edges_parents_fast(self, tuple, object)
    cdef remove_edges_children_fast(self, object, tuple)
    
    cdef reverse_fast(self)
    
    cdef parentless_fast(self)
    cdef childless_fast(self)
    cdef orphans_fast(self)
    
    cdef children_fast(self, object)
    cdef parents_fast(self, object)
    
    cdef traverse_fast(self, object, bint descend=*, bint dfs=*, bint level=*)

    
