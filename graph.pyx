cimport cpython as cpy
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
# Stack
#------------------------------------------------------------------------------

cdef class Stack:
    """ A C-array based PyObject* stack implementation.

    """
    cdef cpy.PyObject** _stack
    cdef int _stack_marker
    cdef int _stack_size

    def __cinit__(self, int init_size):
        cdef void* mem

        mem = <cpy.PyObject**>cpy.PyMem_Malloc(<size_t>(sizeof(cpy.PyObject*) * init_size))
        if mem is NULL:
            raise MemoryError("Could not allocate stack of %s elements" % init_size)

        self._stack = <cpy.PyObject**>mem
        self._stack_marker = -1
        self._stack_size = init_size

    def __dealloc__(self):
        cpy.PyMem_Free(<void*>self._stack)

    cdef inline bint empty(self):
        return self._stack_marker == -1

    cdef _realloc_stack(self):
        cdef int new_size = self._stack_size * 2
        cdef void* mem

        mem = <cpy.PyObject**>cpy.PyMem_Realloc(<void*>self._stack, <size_t>(sizeof(cpy.PyObject*) * new_size))
        if mem is NULL:
            raise MemoryError("Could not reallocate stack of %s elements" % new_size)

        self._stack = <cpy.PyObject**>mem
        self._stack_size = new_size

    cdef push(self, content):
        cdef int marker = self._stack_marker + 1

        if marker >= self._stack_size:
            self._realloc_stack()

        cpy.Py_INCREF(content)
        self._stack[marker] = <cpy.PyObject*>content
        self._stack_marker = marker

    cdef pop(self):
        if self.empty():
            raise ValueError('Pop from empty stack.')

        content = <object>self._stack[self._stack_marker]
        self._stack_marker -= 1
        cpy.Py_DECREF(content)

        return content


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
    cdef list parents
    cdef list children
    cdef bint _check_parent_dups
    cdef bint _check_child_dups

    def __cinit__(self, content):
        self.content = content
        self.children = list()
        self.parents = list() 
        self._check_parent_dups = True
        self._check_child_dups = True

    cdef inline bint has_children(self):
        return (<int>len(self.children)) > 0

    cdef inline bint has_parents(self):
        return (<int>len(self.parents)) > 0

    cdef inline void check_child_dups(self, bint val):
        self._check_child_dups = val
        if val:
            self.children = remove_list_duplicates(self.children)
        
    cdef inline void check_parent_dups(self, bint val):
        self._check_parent_dups = val
        if val:
            self.parents = remove_list_duplicates(self.parents)

    cdef void add_parent(self, _Node parent):
        if self._check_parent_dups:
            if list_contains(self.parents, parent):
                pass
            else:
                self.parents.append(parent)
        else:
            self.parents.append(parent)

    cdef void add_child(self, _Node child):
        if self._check_child_dups:
            if list_contains(self.children, child):
                pass
            else:
                self.children.append(child)
        else:
            self.children.append(child)

    cdef void remove_parent(self, _Node parent):
        if list_contains(self.parents, parent):
            self.parents.remove(parent)

    cdef void remove_child(self, _Node child):
        if list_contains(self.children, child):
            self.children.remove(child)


#------------------------------------------------------------------------------
# Utility functions
#------------------------------------------------------------------------------

cdef inline void push_children(Stack stack, _Node node):
    """ Push the children of `node` onto `stack`. This function
    allows decoupling of the the implementation of _Node children
    from the methods which need to push them onto a stack.

    """
    for obj in node.children:
        stack.push(obj)    


cdef inline void push_parents(Stack stack, _Node node):
    """ Push the parents of `node` onto `stack`. This function
    allows decoupling of the the implementation of _Node parents
    from the methods which need to push them onto a stack.

    """
    for obj in node.parents:
        stack.push(obj)


cdef inline bint list_contains(list objects, object item):
    cdef bint res = False

    for obj in objects:
        if obj is item:
            res = True
            break

    return res 


cdef list remove_list_duplicates(list objects):
    cdef set set_objects = set()
    cdef list new_objects = list()
    
    for obj in objects:
        if obj not in set_objects:
            new_objects.append(obj)
            set_objects.add(obj)
    
    return new_objects


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
    cdef Stack stack

    def __init__(self, _Node start_node):
        self.stack = Stack(2048)
        push_children(self.stack, start_node)

    def __iter__(self):
        return self

    def __next__(self):
        if self.stack.empty():
            raise StopIteration

        curr = <_Node>self.stack.pop()
        push_children(self.stack, curr)

        return curr.content


cdef class _IterDFSUp(object):
    """ A depth first iterator that traverses the DAGraph upward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_dfs_up` method of a DAGraph instance. 
    
    """
    cdef Stack stack

    def __init__(self, _Node start_node):
        self.stack = Stack(2048)
        push_parents(self.stack, start_node)

    def __iter__(self):
        return self

    def __next__(self):
        if self.stack.empty():
            raise StopIteration

        curr = <_Node>self.stack.pop()
        push_parents(self.stack, curr)

        return curr.content

 
cdef class _IterBFSDown(object):
    """ A breadth first iterator that traverses the DAGraph downward
    from a given node. 
    
    This private class is intended for use solely by the DAGraph.
    Instances of this iterator are returned by calling the 
    `iter_bfs_down` method of DAGraph instance. 
    
    """
    cdef object stack

    def __init__(self, _Node start_node):
        self.stack = deque(start_node.children)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.stack:
            raise StopIteration

        curr = <_Node>self.stack.popleft()
        if curr.has_children():
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
        if curr.has_parents():
            self.stack.extend(curr.parents)

        return curr.content


#cdef class _IterDFSDownLevel(object):
#    """ A depth first iterator that traverses the DAGraph downward
#    from a given node, yielding the graph level offset in addition 
#    to the node.
#    
#    This private class is intended for use solely by the DAGraph.
#    Instances of this iterator are returned by calling the 
#    `iter_dfs_down_level` method of a DAGraph instance.
#    
#    """
#    cdef object stack
#    cdef int level
#    cdef object level_markers
#
#    def __init__(self, _Node start_node):
#        self.stack = deque(start_node.children)
#        self.level = 1
#        if self.stack:
#            self.level_markers = deque()
#            self.level_markers.append(self.stack[0])
#        else:
#            self.level_markers = None
#
#    def __iter__(self):
#        return self
#
#    def __next__(self):
#        if not self.stack:
#            raise StopIteration
#
#        ret_level = self.level
#
#        curr = self.stack.pop()
#
#        if curr is self.level_markers[-1]:
#            if not (<_Node>curr).has_children:
#                self.level -= 1
#            self.level_markers.pop()
#            
#        if (<_Node>curr).has_children:
#            idx = len(self.stack)
#            self.stack.extend((<_Node>curr).children)
#            self.level_markers.append(self.stack[idx])
#            self.level += 1
#
#        return (<_Node>curr).content, ret_level
#
#
#cdef class _IterDFSUpLevel(object):
#    """ A depth first iterator that traverses the DAGraph upward
#    from a given node, yielding the graph level offset in addition 
#    to the node.
# 
#    
#    This private class is intended for use solely by the DAGraph.
#    Instances of this iterator are returned by calling the 
#    `iter_dfs_up_level` method of a DAGraph instance. 
#    
#    """
#    cdef object stack
#    cdef int level
#    cdef object level_markers
#
#    def __init__(self, _Node start_node):
#        self.stack = deque(start_node.parents)
#        self.level = 1
#        if self.stack:
#            self.level_markers = deque()
#            self.level_markers.append(self.stack[0])
#        else:
#            self.level_markers = None
#
#    def __iter__(self):
#        return self
#
#    def __next__(self):
#        if not self.stack:
#            raise StopIteration
#
#        ret_level = self.level
#
#        curr = self.stack.pop()
#
#        if curr is self.level_markers[-1]:
#            if not (<_Node>curr).has_parents:
#                self.level -= 1
#            self.level_markers.pop()
#
#        if (<_Node>curr).has_parents():
#            idx = len(self.stack)
#            self.stack.extend((<_Node>curr).parents)
#            self.level_markers.append(self.stack[idx])
#            self.level += 1
#
#        return (<_Node>curr).content, ret_level
#
# 
#cdef class _IterBFSDownLevel(object):
#    """ A breadth first iterator that traverses the DAGraph downward
#    from a given node, yielding the graph level offset in addition 
#    to the node.
#
#   
#    This private class is intended for use solely by the DAGraph.
#    Instances of this iterator are returned by calling the 
#    `iter_bfs_down_level` method of DAGraph instance.
#    
#    """
#    cdef object stack
#    cdef int level
#    cdef object level_marker
#
#    def __init__(self, _Node start_node):
#        self.stack = deque(start_node.children)
#        self.level = 1
#        if self.stack:
#            self.level_marker = self.stack[-1]
#        else:
#            self.level_marker = None
#
#    def __iter__(self):
#        return self
#
#    def __next__(self):
#        if not self.stack:
#            raise StopIteration
#
#        ret_level = self.level
#
#        curr = self.stack.popleft()
#
#        if (<_Node>curr).has_children():
#            self.stack.extend((<_Node>curr).children)
#
#        if curr is self.level_marker:
#            self.level += 1
#            if self.stack:
#                self.level_marker = self.stack[-1]
#            else:
#                self.level_marker = None
#
#        return (<_Node>curr).content, ret_level
# 
#
#cdef class _IterBFSUpLevel(object):
#    """ A breadth first iterator that traverses the DAGraph upward
#    from a given node, yielding the graph level offset in addition 
#    to the node.
# 
#    
#    This private class is intended for use solely by the DAGraph.
#    Instances of this iterator are returned by calling the 
#    `iter_bfs_up_level` method of DAGraph instance.
#    
#    """
#    cdef object stack
#    cdef int level
#    cdef object level_marker
#
#    def __init__(self, _Node start_node):
#        self.stack = deque(start_node.parents)
#        self.level = 1
#        if self.stack:
#            self.level_marker = self.stack[-1]
#        else:
#            self.level_marker = None
#
#    def __iter__(self):
#        return self
#
#    def __next__(self):
#        if not self.stack:
#            raise StopIteration
#
#        ret_level = self.level
#
#        curr = self.stack.popleft()
#
#        if (<_Node>curr).has_parents():
#            self.stack.extend((<_Node>curr).parents)
#
#        if curr is self.level_marker:
#            self.level += 1
#            if self.stack:
#                self.level_marker = self.stack[-1]
#            else:
#                self.level_marker = None
#
#        return (<_Node>curr).content, ret_level


cdef class _IterParentless(object):
    
    cdef object node_iterator

    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <_Node>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents() and cnode.has_children():
                return cnode.content


cdef class _IterChildless(object):
    
    cdef object node_iterator

    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <_Node>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if cnode.has_parents() and not cnode.has_children():
                return cnode.content


cdef class _IterOrphans(object):
    
    cdef object node_iterator

    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <_Node>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents() and not cnode.has_children():
                return cnode.content


cdef class _IterContent(object):
    
    cdef object node_iterator

    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        cnode = <_Node>cpy.PyIter_Next(self.node_iterator)
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
    cdef dict _graph_nodes
    cdef bint _detect_cycles

    def __cinit__(self):
        self._graph_nodes = {}
        self._detect_cycles = True

    #------------------------------------------------------------------
    # Fast C-only methods
    #------------------------------------------------------------------
    cdef inline bint contains(self, content):
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
        cdef bint res = False

        if content in self._graph_nodes:
            res = True

        return res
    
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
        return <_Node>self._graph_nodes[content]

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
        out : tuple
            If a cycle is detected, the tuple will contain the 
            start node of the search and the node visited just 
            before the completion of the cycle (i.e. revisiting 
            the start node). Otherwise, the tuple will be empty.

        """
        start = node.content
        last = None

        for child in _IterDFSDown(node):
            if child is start:
                return (start, last)
            else:
                last = child
        
        return ()

    #------------------------------------------------------------------
    # Python special methods
    #------------------------------------------------------------------
    def __iter__(self):
        """ Returns an iterator that will yield the content reference
        by every node in the graph, in no particular order.

        """
        return self._graph_nodes.iterkeys()

    def __contains__(self, item):
        """ Returns True if `item` is referenced by a node in the graph;
        False otherwise.

        """
        return item in self._graph_nodes
   
    def __len__(self):
        """ Returns the number of nodes in the graph.

        """
        return len(self._graph_nodes)

    #------------------------------------------------------------------
    # Graph behavior modification
    #------------------------------------------------------------------
    def detect_cycles(self, bint val):
        """ cycle_detect(bool)

        Turn on/off the graph's cycle detector.

        By default, the graph's cycle detector is turned on.
        The graph is checked for cycles each time an edge is added,
        and a CycleError is raised if one is detected. The cycle 
        detector should only be turned off if there is absolute
        certainty that the edges being added will not cause a cycle
        and the overhead of running the cycle detector is excessive.

        Parameters
        ----------
        val : boolean
            Should the cycle detector be on or off.

        Returns
        -------
        None

        """
        self._detect_cycles = val

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
        if self.contains(node):
            return

        graph_node = _Node(node)
        self._graph_nodes[node] = graph_node

    cpdef delete_node(self, node):
        """ delete_node(node)
        
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
        if not self.contains(node):
            return 

        graph_node = self.get_node(node)
        
        for parent in graph_node.parents:
            parent.remove_child(graph_node)

        for child in graph_node.children:
            child.remove_parent(graph_node)

        del self._graph_nodes[node]

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

        if self._detect_cycles:
            cycle = self.cycle_detect(source_node)
            if cycle:
                raise CycleError(cycle)

    cpdef add_edges_parents(self, tuple sources, target):
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
        sources : tuple of hashable objects
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
        cdef bint disable_dup_checking
        cdef _Node source_node, target_node

        if not self.contains(target):
            self.add_node(target) 

        target_node = self.get_node(target)
        disable_dup_checking = (len(sources) >= 1000)

        if disable_dup_checking:
            # 1000 elements is approx. the number necessary such that
            # checking for duplicates with set operations results
            # in a gain, vs a linear search over the children.
            target_node.check_parent_dups(False)

        for source in sources:
            if not self.contains(source):
                self.add_node(source)
            source_node = self.get_node(source)
            target_node.add_parent(source_node)
            source_node.add_child(target_node)
        
        if disable_dup_checking:
            # re-enabling duplicate checking will uniqify the 
            # the child list
            target_node.check_parent_dups(True)

        if self._detect_cycles:
            for source in sources:
                source_node = self.get_node(source)
                cycle = self.cycle_detect(source_node)
                if cycle:
                    raise CycleError(cycle)

    cpdef add_edges_children(self, source, tuple targets):
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
        targets : tuple of hashable objects
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
        cdef bint disable_dup_checking
        cdef _Node target_node, source_node

        if not self.contains(source):
            self.add_node(source) 
        
        source_node = self.get_node(source)
        disable_dup_checking = (len(targets) >= 1000)

        if disable_dup_checking:
            # 1000 elements is approx. the number necessary such that
            # checking for duplicates with set operations results
            # in a gain, vs a linear search over the children.
            source_node.check_child_dups(False)

        for target in targets:
            if not self.contains(target):
                self.add_node(target)
            target_node = self.get_node(target)
            target_node.add_parent(source_node)
            source_node.add_child(target_node)

        if disable_dup_checking:
            # re-enabling duplicate checking will uniqify the 
            # the child list
            source_node.check_child_dups(True)

        if self._detect_cycles:
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

    cpdef remove_edges_parents(self, tuple sources, target):
        """ remove_edges_parents(sources, target)

        Remove many-to-ones edge.

        Disconnect nodes in the graph by removing the many-to-one 
        edges between `sources` and `target`. If any of the given 
        nodes do not exist in the graph, the function has no effect.
        If a specified edge does not exist, then it is ignored.

        Parameters
        ----------
        sources : tuple of hashable objects
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

    cpdef remove_edges_children(self, source, tuple targets):
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
        for node in self._graph_nodes.itervalues():
            cnode = <_Node>node
            cnode.children, cnode.parents = cnode.parents, cnode.children

    #------------------------------------------------------------------
    # Graph introspection
    #------------------------------------------------------------------
#    cpdef parentless(self):
#        """ parentless()
#
#        Returns an iterator which returns nodes in the graph without parents.
#
#        Nodes without parents are root/toplevel nodes. If the graph
#        were traversed in a upward direction, these would be the
#        terminating nodes.
#
#        Parameters
#        ----------
#        None
#
#        Returns
#        -------
#        out : iterator
#            An iterator which returns nodes in the graph with no parents.
#
#        """
#        return _IterParentless(self._graph_nodes.itervalues())
#
#    cpdef childless(self):
#        """ childless()
#
#        Returns an iterator which returns nodes in the graph without children.
#
#        Nodes without children are leaf/terminal nodes. If the graph
#        were traversed in a downward direction, these would be the
#        terminating nodes.
#
#        Parameters
#        ----------
#        None
#
#        Returns
#        -------
#        out : iterator
#            An iterator which returns nodes in the graph with no children.
#
#        """
#        return _IterChildless(self._graph_nodes.itervalues())
#
#    cpdef orphans(self):
#        """ orphans()
#
#        Returns an iterator which returns nodes in the graph without 
#        parents or children.
#
#        Nodes without parents children are orphan nodes. These nodes
#        are unreachable by traversing the graph from any other node.
#
#        Parameters
#        ----------
#        None
#
#        Returns
#        -------
#        out : iterator
#            An iterator which returns nodes in the graph with no 
#            parents or children.
#
#        """
#        return _IterOrphans(self._graph_nodes.itervalues())
#
#    cpdef children(self, node):
#        """ children(node)
#
#        Return the children of a given node.
#
#        The children of a node are those nodes directly below 
#        the given node in the graph. If the given node is not 
#        contained in the graph, a GraphError is raised.
#
#        Parameters
#        ----------
#        node : hashable object
#            The parent node.
#
#        Returns
#        -------
#        out : list
#            The children of the given node. Will be empty if the
#            node has no children.
#
#        """
#        if not self.contains(node):
#            raise GraphError("Node `%s` does not exist in the graph." % node)
#
#        graph_node = self.get_node(node)
#        return _IterContent(iter(graph_node.parents))
#
#    cpdef parents(self, node):
#        """ parents(node)
#
#        Return the parents of a given node.
#
#        The parents of a node are those nodes directly above 
#        the given node in the graph. A node may have more than 
#        one parent. If the given node is not contained in the 
#        graph, a GraphError is raised.
#
#        Parameters
#        ----------
#        node : hashable object
#            The child node.
#
#        Returns
#        -------
#        out : list
#            The parents of the given node. Will be empty if the
#            node has no parents.
#
#        """
#        if not self.contains(node):
#            raise GraphError("Node `%s` does not exist in the graph." % node)
#        
#        graph_node = self.get_node(node)
#        return _IterContent(iter(graph_node.parents))

    #------------------------------------------------------------------
    # Graph traversal
    #------------------------------------------------------------------
    cpdef traverse(self, node, bint descend=True, bint dfs=True, bint level=False):
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
                    return _IterDFSDown(graph_node)
                else:
                    return _IterDFSDown(graph_node)
            else:
                if level:
                    return _IterBFSDown(graph_node)
                else:
                    return _IterBFSDown(graph_node)
        else:
            if dfs:
                if level:
                    return _IterDFSUp(graph_node)
                else:
                    return _IterDFSUp(graph_node)
            else:
                if level:
                    return _IterBFSUp(graph_node)
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


