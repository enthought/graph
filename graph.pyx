cimport python as py

from collections import deque

    
#------------------------------------------------------------------------------
# Exception Types
#------------------------------------------------------------------------------

class GraphError(Exception):
    pass


class CycleError(GraphError):

    def __init__(self, cycle):
        super(CycleError, self).__init__('Cycle detected: %s. The dfs that '
                                         'led to the cycle can be retrieved '
                                         'from the `cycle` attribute of this '
                                         'exception.' % cycle)
        self.cycle = cycle


#------------------------------------------------------------------------------
# Node Type
#------------------------------------------------------------------------------

cdef class _Node:
    """ This private class represents a node on the DAGraph. 
    
    It carries a pointer to the node's contents, which must be a 
    hashable Python object, and two sets containing references to 
    parent and child _Node instances. This type is intended for 
    use solely by the DAGraph class. 
    
    """
    cdef object content
    cdef set parents
    cdef set children
    cdef int has_parents
    cdef int has_children

    def __cinit__(self, content):
        self.content = content
        self.parents = set()
        self.children = set()
        self.has_parents = 0
        self.has_children = 0

    cdef add_parent(self, _Node parent):
        self.parents.add(<object>parent)
        self.has_parents = 1

    cdef add_child(self, _Node child):
        self.children.add(<object>child)
        self.has_children = 1

    cdef remove_parent(self, _Node parent):
        self.parents.discard(<object>parent)
        if not self.parents:
            self.has_parents = 0

    cdef remove_child(self, _Node child):
        self.children.discard(<object>child)
        if not self.children:
            self.has_children = 0


#------------------------------------------------------------------------------
# Iterators
#------------------------------------------------------------------------------

cdef class _IterDFSDown(object):
    """ A depth first iterator that traverses the DAGraph downward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_dfs_down` method of a DAGraph instance. 
    
    """
    cdef object stack

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.children)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        curr = <_Node>self.stack.pop()
        if curr.has_children:
            self.stack.extend(curr.children)

        return curr.content


cdef class _IterDFSUp(object):
    """ A depth first iterator that traverses the DAGraph upward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_dfs_up` method of a DAGraph instance. 
    
    """
    cdef object stack

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.parents)

    def __iter__(self):
        return self

    def __next__(self):
        if self.stack:
            raise StopIteration

        curr = <_Node>self.stack.pop()
        if curr.has_parents:
            self.stack.extend(curr.parents)

        return curr.content

 
cdef class _IterBFSDown(object):
    """ A breadth first iterator that traverses the DAGraph downward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_bfs_down` method of DAGraph instance. 
    
    """
    cdef object stack
    cdef int stack_marker

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.children)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        curr = <_Node>self.stack.popleft()
        if curr.has_children:
            self.stack.extend(curr.children)

        return curr.content
 

cdef class _IterBFSUp(object):
    """ A breadth first iterator that traverses the DAGraph upward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_bfs_up` method of DAGraph instance.
    
    """
    cdef object stack

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.parents)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        curr = <_Node>self.stack.popleft()
        if curr.has_parents:
            self.stack.extend(curr.parents)

        return curr.content


cdef class _IterDFSDownLevel(object):
    """ A depth first iterator that traverses the DAGraph downward
    from a given node, yielding the graph level offset in addition 
    to the node.
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_dfs_down_level` method of a DAGraph instance.
    
    """
    cdef object stack
    cdef int level
    cdef object level_markers

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.children)
        self.level = 1
        if self.stack:
            self.level_markers = deque()
            self.level_markers.append(self.stack[0])
        else:
            self.level_markers = None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        ret_level = self.level

        curr = self.stack.pop()

        if curr is self.level_markers[-1]:
            if not (<_Node>curr).has_children:
                self.level -= 1
            self.level_markers.pop()
            
        if (<_Node>curr).has_children:
            idx = len(self.stack)
            self.stack.extend((<_Node>curr).children)
            self.level_markers.append(self.stack[idx])
            self.level += 1

        return (<_Node>curr).content, ret_level


cdef class _IterDFSUpLevel(object):
    """ A depth first iterator that traverses the DAGraph upward
    from a given node, yielding the graph level offset in addition 
    to the node.
 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_dfs_up_level` method of a DAGraph instance. 
    
    """
    cdef object stack
    cdef int level
    cdef object level_markers

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.parents)
        self.level = 1
        if self.stack:
            self.level_markers = deque()
            self.level_markers.append(self.stack[0])
        else:
            self.level_markers = None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        ret_level = self.level

        curr = self.stack.pop()

        if curr is self.level_markers[-1]:
            if not (<_Node>curr).has_parents:
                self.level -= 1
            self.level_markers.pop()

        if (<_Node>curr).has_parents:
            idx = len(self.stack)
            self.stack.extend((<_Node>curr).parents)
            self.level_markers.append(self.stack[idx])
            self.level += 1

        return (<_Node>curr).content, ret_level

 
cdef class _IterBFSDownLevel(object):
    """ A breadth first iterator that traverses the DAGraph downward
    from a given node, yielding the graph level offset in addition 
    to the node.

   
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_bfs_down_level` method of DAGraph instance.
    
    """
    cdef object stack
    cdef int level
    cdef object level_marker

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.children)
        self.level = 1
        if self.stack:
            self.level_marker = self.stack[-1]
        else:
            self.level_marker = None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        ret_level = self.level

        curr = self.stack.popleft()

        if (<_Node>curr).has_children:
            self.stack.extend((<_Node>curr).children)

        if curr is self.level_marker:
            self.level += 1
            if self.stack:
                self.level_marker = self.stack[-1]
            else:
                self.level_marker = None

        return (<_Node>curr).content, ret_level
 

cdef class _IterBFSUpLevel(object):
    """ A breadth first iterator that traverses the DAGraph upward
    from a given node, yielding the graph level offset in addition 
    to the node.
 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_bfs_up_level` method of DAGraph instance.
    
    """
    cdef object stack
    cdef int level
    cdef object level_marker

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.parents)
        self.level = 1
        if self.stack:
            self.level_marker = self.stack[-1]
        else:
            self.level_marker = None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        ret_level = self.level

        curr = self.stack.popleft()

        if (<_Node>curr).has_parents:
            self.stack.extend((<_Node>curr).parents)

        if curr is self.level_marker:
            self.level += 1
            if self.stack:
                self.level_marker = self.stack[-1]
            else:
                self.level_marker = None

        return (<_Node>curr).content, ret_level


cdef class _BaseNodeIterator(object):
    
    cdef object node_iterator

    def __cinit__(self, iterator):
        if not py.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


cdef class _IterParentless(_BaseNodeIterator):

    def __next__(self):
        while True:
            cnode = <_Node>py.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents and cnode.has_children:
                return cnode.content


cdef class _IterChildless(_BaseNodeIterator):

    def __next__(self):
        while True:
            cnode = <_Node>py.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if cnode.has_parents and not cnode.has_children:
                return cnode.content


cdef class _IterOrphans(_BaseNodeIterator):

    def __next__(self):
        while True:
            cnode = <_Node>py.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents and not cnode.has_children:
                return cnode.content


cdef class _IterContent(_BaseNodeIterator):

    def __next__(self):
        cnode = <_Node>py.PyIter_Next(self.node_iterator)
        if <void*>cnode == NULL:
            raise StopIteration
  
        return cnode.content


#------------------------------------------------------------------------------
# DAG type
#------------------------------------------------------------------------------

cdef class DAGraph(object):
    """ A directed acyclic graph.

    cycle detection is on by default. Turn off with `disable_cycle_detection`
    turn on with `enable_cycle_detection`

    <need better docs> 
    
    """

    #------------------------------------------------------------------
    # Initialization
    #------------------------------------------------------------------
    cdef dict graph_nodes
    cdef int detect_cycles

    def __cinit__(self):
        self.graph_nodes = {}
        self.detect_cycles = 1

    #------------------------------------------------------------------
    # Fast C-only methods
    #------------------------------------------------------------------
    cdef inline int contains(self, content):
        """ Return whether the graph contains a node referencing `content`.

        If the graph contains a _Node which holds a reference to the
        Python object `content`, return 1. Otherwise, return 0.

        Parameters
        ----------
        content : hashable object
            The graph will be checked for refernces to this object.
        
        Returns
        -------
        out : int 
            1 if `content` is in the graph, 0 otherwise. 

        """
        if content not in self.graph_nodes:
            return 0
        return 1
    
    cdef inline _Node get_node(self, content):
        """ Return the _Node instance which contains `content`.

        Return the _Node instance in the graph which contains a 
        reference to the given object. Will raise a KeyError
        if the object is not referenced by any node.

        Parameters
        ----------
        content : hashable object
            The object which represents a "node" to the user.

        Returns
        -------
        out : _Node instance
            The node which holds a references `content`.

        """
        return <_Node>self.graph_nodes[content]

    cdef void delete_node(self, content):
        """ Delete a node from the graph.

        If the graph contains a node which references the object
        `content`, delete that node and any edges to that node. 
        Otherwise, do nothing.

        Parameters
        ----------
        content : hashable object
            The _Node instance in the graph containing a reference
            to this object will be deleting along with any of its
            edges.

        Returns
        -------
        None

        """
        if not self.contains(content):
            return 

        node = self.get_node(content)
        
        for parent in node.parents:
            parent.remove_child(node)

        for child in node.children:
            child.remove_parent(node)

        del self.graph_nodes[content]

    cdef cycle_detect(self, _Node node):
        """ Detect a cycle in the graph starting at `node`.

        Performs a depth first search starting at `node` which
        continues until the search terminates or the search 
        revisits the starting node, thus indicating a cycle.

        Parameters
        ----------
        node : _Node instance
            The node to begin the cycle search.

        Returns
        -------
        out : list
            If a cycle is detected, the list will contain the 
            nodal contents of each node in the depth first 
            search with first and last element being the same 
            object. Otherwise, the list will be empty.

        """
        start = node.content
        res = [start]
        for child in _IterDFSDown(node):
            res.append(child)
            if child is start:
                return res
        else:
            return []

    #------------------------------------------------------------------
    # Python special methods
    #------------------------------------------------------------------
    def __iter__(self):
        """ Returns an iterator that will yield the content reference
        by every node in the graph, in no particular order.

        """
        return self.graph_nodes.iterkeys()

    def __contains__(self, item):
        """ Returns True if `item` is referenced by a node in the graph;
        False otherwise.

        """
        return item in self.graph_nodes
   
    def __len__(self):
        """ Returns the number of nodes in the graph.

        """
        return len(self.graph_nodes)

    #------------------------------------------------------------------
    # Graph behavior modification
    #------------------------------------------------------------------
    def disable_cycle_detection(self):
        """ disable_cycle_detection()

        Turn off the graph's cycle detector.

        By default, the graph's cycle detector is turned on.
        The graph is checked for cycles each time an edge is added,
        and a CycleError is raised if one is detected. The cycle 
        detector should only be turned off if there is absolute
        certainty that the edges being added will not cause a cycle
        and the overhead of running the cycle detector is excessive.

        Parameters
        ----------
        None

        Returns
        -------
        None

        See Also
        --------
        enable_cycle_detection : Turn on the graph's cycle detector.

        """
        self.detect_cycles = 0

    def enable_cycle_detection(self):
        """ enable_cycle_detection()

        Turn on the graph's cycle detector.

        By default, the graph's cycle detector is turned on.
        The graph is checked for cycles each time an edge is added,
        and a CycleError is raised if one is detected. The cycle 
        detector should be left on unless there is absolute certainty 
        that the edges being added will not cause a cycle and the 
        overhead of running the cycle detector is excessive.

        Parameters
        ----------
        None

        Returns
        -------
        None

        See Also
        --------
        disable_cycle_detection : Turn off the graph's cycle detector.

        """
        self.detect_cycles = 1

    #------------------------------------------------------------------
    # Graph structure modification
    #------------------------------------------------------------------
    cpdef add_node(self, node):
        """ add_node(node)

        Add a node to the graph. 

        Add a node to the graph which will hold a (non-weak) reference
        to `node`. If `node` is already referenced by the graph, this 
        function has no effect. This function will raise a GraphError 
        if `node` is not hashable.

        Parameters
        ----------
        node : hashable object
            The created graph node will contain a non-weak reference
            to this object.

        Returns
        -------
        None

        """
        try:
            hash(node)
        except TypeError:
            raise GraphError("Graph nodes must be hashable python objects.")
       
        if self.contains(node):
            return

        graph_node = _Node(node)
        self.graph_nodes[node] = graph_node

    cpdef remove_node(self, node):
        """ remove_node(node)
        
        Delete a node from the graph.

        If the graph contains a reference to `node`, delete that node
        and any edges to that node; otherwise, do nothing.

        Parameters
        ----------
        node : hashable object
            The node referenced by the graph.

        Returns
        -------
        None

        """
        self.delete_node(node)

    cpdef add_edge(self, source, target):
        """ add_edge(source, target)

        Add an edge between two nodes.

        Connect two nodes in the graph by creating an edge from
        `source` pointing towards `target`. If a given node does
        not exist in the graph, it will be added. At the end of 
        this operation, `source` will be a parent of `target`,
        and `target` will be a child of `source`. If cycle detection
        is turned on, and adding this edge creates a cycle, then
        a CycleError will be raised, but the edge will still be
        added.

        Parameters
        ----------
        source : hashable object
            The parent node.
        target : hashable object
            The child node.

        Returns
        -------
        None

        See Also
        --------
        enable_cycle_detection : Turn on the cycle detector.
        disable_cycle_detection : Turn off the cycle detector.
        remove_edge : Remove an edge from the graph.

        """
        if not self.contains(source):
            self.add_node(source)

        if not self.contains(target):
            self.add_node(target)

        source_node = self.get_node(source)
        target_node = self.get_node(target)
        
        source_node.add_child(target_node)
        target_node.add_parent(source_node)
        if self.detect_cycles:
            cycle = self.cycle_detect(source_node)
            if cycle:
                raise CycleError(cycle)

    cpdef add_edges_parents(self, sources, target):
        """ add_edges_parents(sources, target)

        Add an edge between many-to-one nodes.

        Connect nodes in the graph by creating an edge from each
        node in `sources` pointing towards `target`. If a given 
        node does not exist in the graph, it will be added. At the 
        end of this operation, each node in `sources` will be a parent 
        of `target`, and `target` will be a child of each node in 
        `sources`. If cycle detection is turned on, and adding any of
        these edges creates a cycle, then a CycleError will be raised, 
        but the edges will still be added up to and including the edge
        that caused the cycle.

        Parameters
        ----------
        sources : iterable of hashable objects
            The parent nodes.
        target : hashable object
            The child node.

        Returns
        -------
        None

        See Also
        --------
        enable_cycle_detection : Turn on the cycle detector.
        disable_cycle_detection : Turn off the cycle detector.
        add_edges_children : Add one-to-many edges.
        remove_edges_parents : Remove many-to-one edges.
        remove_edges_children : Remove one-to-many edges.

        """
        for source in sources:
            if not self.contains(source):
                self.add_node(source) 

        if not self.contains(target):
            self.add_node(target) 

        target_node = self.get_node(target)
        for source in sources:
            source_node = self.get_node(source)
            target_node.add_parent(source_node)
            source_node.add_child(target_node)
            if self.detect_cycles:
                cycle = self.cycle_detect(source_node)
                if cycle:
                    raise CycleError(cycle)

    cpdef add_edges_children(self, source, targets):
        """ add_edge_parents(source, targets)

        Add an edge between one-to-many nodes.

        Connect nodes in the graph by creating an edge from `source`
        pointing towards each node in `targets`. If a given node does 
        not exist in the graph, it will be added. At the end of this 
        operation, each node in `targets` will be a child of `source`, 
        and `source` will be a parent of each node in `targets`. If 
        cycle detection is turned on, and adding any of these edges 
        creates a cycle, then a CycleError will be raised, but the 
        edges will still be added up to and including the edge that 
        caused the cycle.

        Parameters
        ----------
        source : hashable object
            The parent node.
        targets : iterable of hashable objects
            The child nodes.

        Returns
        -------
        None

        See Also
        --------
        enable_cycle_detection : Turn on the cycle detector.
        disable_cycle_detection : Turn off the cycle detector.
        add_edges_parents : Add many-to-one edges.
        remove_edges_parents : Remove many-to-one edges.
        remove_edges_children : Remove one-to-many edges.

        """
        if not self.contains(source):
            self.add_node(source) 
        
        for target in targets:
            if not self.contains(target):
                self.add_node(target) 

        source_node = self.get_node(source)
        for target in targets:
            target_node = self.get_node(target)
            target_node.add_parent(source_node)
            source_node.add_child(target_node)
            if self.detect_cycles:
                cycle = self.cycle_detect(source_node)
                if cycle:
                    raise CycleError(cycle)

    cpdef remove_edge(self, source, target):
        """ remove_edge(source, target)

        Remove an edge between two nodes.

        Disconnect two nodes in the graph by removing the edge 
        between them. If any of the given nodes do not exist in the 
        graph, or if the specified edge does not exist, the function 
        has no effect.

        Parameters
        ----------
        source : hashable object
            The parent node.
        target : hashable object
            The child node.

        Returns
        -------
        None

        See Also
        --------
        add_edge : Add an edge to the graph.

        """
        if not self.contains(source):
            return

        if not self.contains(target):
            return

        source_node = self.get_node(source)
        target_node = self.get_node(target)

        target_node.remove_parent(source_node)
        source_node.remove_child(target_node)

    cpdef remove_edges_parents(self, sources, target):
        """ remove_edges_parents(sources, target)

        Remove many-to-ones edge.

        Disconnect nodes in the graph by removing the many-to-one 
        edges between `sources` and `target`. If any of the given 
        nodes do not exist in the graph, the function has no effect.
        If a specified edge does not exist, then it is ignored.

        Parameters
        ----------
        sources : iterable of hashable object
            The parent nodes.
        target : hashable object
            The child node.

        Returns
        -------
        None

        See Also
        --------
        add_edges_parents : Add many-to-one edges to the graph.
        add_edges_children : Add one-to-many edges to the graph.
        remove_edges_children : Remove one-to-many edges from the graph.

        """
        for source in sources:
            if not self.contains(source):
                return

        if not self.contains(target):
            return
        
        target_node = self.get_node(target)
        for source in sources:
            source_node = self.get_node(source)
            target_node.remove_parent(source_node)
            source_node.remove_child(target_node)

    cpdef remove_edges_children(self, source, targets):
        """ remove_edges_children(source, targets)

        Remove one-to-many edges.

        Disconnect nodes in the graph by removing the one-to-many 
        edges between `source` and `targets`. If any of the given 
        nodes do not exist in the graph, the function has no effect.
        If a specified edge does not exist, then it is ignored.

        Parameters
        ----------
        source : hashable object
            The parent node.
        target : iterable of hashable objects
            The child nodes.

        Returns
        -------
        None

        See Also
        --------
        add_edges_parents : Add many-to-one edges to the graph.
        add_edges_children : Add one-to-many edges to the graph.
        remove_edges_parents : Remove many-to-one edges from the graph.

        """
        if not self.contains(source):
            return

        for target in targets:
            if not self.contains(target):
                return

        source_node = self.get_node(source)
        for target in targets:
            target_node = self.get_node(target)
            target_node.remove_parent(source_node)
            source_node.remove_child(target_node)

    cpdef reverse(self):
        """ reverse()

        Reverse the graph in-place.

        The graph is reversed by visting every node and exchanging
        parents for children. Traversing the graph from a given 
        node in an upward direction is equivalent to reversing the 
        graph and then traversing from that node in a downward 
        direction.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        for node in self.graph_nodes.itervalues():
            cnode = <_Node>node
            cnode.children, cnode.parents = cnode.parents, cnode.children

    #------------------------------------------------------------------
    # Graph introspection
    #------------------------------------------------------------------
    cpdef parentless(self):
        """ parentless()

        Returns an iterator which returns nodes in the graph without parents.

        Nodes without parents are root/toplevel nodes. If the graph
        were traversed in a upward direction, these would be the
        terminating nodes.

        Parameters
        ----------
        None

        Returns
        -------
        out : iterator
            An iterator which returns nodes in the graph with no parents.

        """
        return _IterParentless(self.graph_nodes.itervalues())

    cpdef childless(self):
        """ childless()

        Returns an iterator which returns nodes in the graph without children.

        Nodes without children are leaf/terminal nodes. If the graph
        were traversed in a downward direction, these would be the
        terminating nodes.

        Parameters
        ----------
        None

        Returns
        -------
        out : iterator
            An iterator which returns nodes in the graph with no children.

        """
        return _IterChildless(self.graph_nodes.itervalues())

    cpdef orphans(self):
        """ orphans()

        Returns an iterator which returns nodes in the graph without 
        parents or children.

        Nodes without parents children are orphan nodes. These nodes
        are unreachable by traversing the graph from any other node.

        Parameters
        ----------
        None

        Returns
        -------
        out : iterator
            An iterator which returns nodes in the graph with no 
            parents or children.

        """
        return _IterOrphans(self.graph_nodes.itervalues())

    cpdef children(self, node):
        """ children(node)

        Return the children of a given node.

        The children of a node are those nodes directly below 
        the given node in the graph. If the given node is not 
        contained in the graph, a GraphError is raised.

        Parameters
        ----------
        node : hashable object
            The parent node.

        Returns
        -------
        out : list
            The children of the given node. Will be empty if the
            node has no children.

        """
        if not self.contains(node):
            raise GraphError("Node `%s` does not exist in the graph." % node)

        graph_node = self.get_node(node)
        return _IterContent(iter(graph_node.parents))

    cpdef parents(self, node):
        """ parents(node)

        Return the parents of a given node.

        The parents of a node are those nodes directly above 
        the given node in the graph. A node may have more than 
        one parent. If the given node is not contained in the 
        graph, a GraphError is raised.

        Parameters
        ----------
        node : hashable object
            The child node.

        Returns
        -------
        out : list
            The parents of the given node. Will be empty if the
            node has no parents.

        """
        if not self.contains(node):
            raise GraphError("Node `%s` does not exist in the graph." % node)
        
        graph_node = self.get_node(node)
        return _IterContent(iter(graph_node.parents))

    #------------------------------------------------------------------
    # Graph traversal
    #------------------------------------------------------------------
    cpdef traverse(self, node, descend=True, dfs=True, level=False):
        """ traverse(node, descend=True, dfs=True, level=False)

        Returns an iterator that performs a search through the graph.

        Returns an iterator that returns nodes in the graph by walking
        the graph starting at `node` and traversering the graph 
        according to the specified arguments.
        
        If `node` does not exist in the graph, a GraphError will be raised.

        Parameters
        ----------
        node : hashable object
            The node at which to start the traversal.
        descend : boolean
            If True the traversal will descend the graph. If False,
            the traversal will ascend the graph.
        dfs : boolean
            If True the traversal will be a depth-first search. If False,
            the traversal will be a breadth-first search.
        level : boolean
            If True, the return value will be (node, level) where level
            is the graph offset level of the current node from the starting
            node.

        Returns
        -------
        out : iterator
            An iterator which returns the nodes visited in the course 
            of the traversal according to the given arguments.
        
        See Also
        --------
        filter : An traversing iterator which calls a callback at each node.

        """
        if not self.contains(node):
            raise GraphError("Node `%s` does not exist in the graph." % node)

        graph_node = self.get_node(node)

        if descend:
            if dfs:
                if level:
                    return _IterDFSDownLevel(graph_node)
                else:
                    return _IterDFSDown(graph_node)
            else:
                if level:
                    return _IterBFSDownLevel(graph_node)
                else:
                    return _IterBFSDown(graph_node)
        else:
            if dfs:
                if level:
                    return _IterDFSUpLevel(graph_node)
                else:
                    return _IterDFSUp(graph_node)
            else:
                if level:
                    return _IterBFSUpLevel(graph_node)
                else:
                    return _IterBFSUp(graph_node)

    #------------------------------------------------------------------
    # Graph filtering
    #------------------------------------------------------------------
    cpdef filter(self, node, filter_func, descend=True, dfs=True, level=False):
        """ filter(node, filter_func, descend=True, dfs=True, level=False)

        Traverse the graph calling filter_func at each node.

        Traverse the graph according to the specified arguments and
        call filter_func at every visited node. The signature of 
        `filter_func` should be:
          
            filter_func(node[, level], graph) 
        
        where the `level` argument is provided if the level=True for 
        this function. If `node` does not exist in the graph, a GraphError 
        will be raised.

        Parameters
        ----------
        node : hashable object
            The node at which to start the traversal.
        filter_func : callable
            The callback function to call at each visited node.
        descend : boolean
            If True the traversal will descend the graph. If False,
            the traversal will ascend the graph.
        dfs : boolean
            If True the traversal will be a depth-first search. If False,
            the traversal will be a breadth-first search.
        level : boolean
            If True, the filter_func will be called with (node, level, graph)
            where level is the graph offset level of the current node from 
            the starting node. If False, the filter_func will be called
            with (node, graph)

        Returns
        -------
        None

        See Also
        --------
        traverse : An iterator for traversing the graph.

        """
        for res in self.traverse(node, descend, dfs, level):
            try:
                if level:
                    node, level = res
                    filter_func(node, level, self)
                else:
                    filter_func(res, self)
            except StopIteration:
                break


