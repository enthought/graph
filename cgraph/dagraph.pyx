cimport cpython as cpy

from c_collections cimport Stack
from iterators cimport (IterDFSDown, IterDFSUp, IterBFSDown, IterBFSUp,
                        IterDFSDownLevel, IterDFSUpLevel, IterBFSDownLevel,
                        IterBFSUpLevel, IterParentless, IterChildless,
                        IterOrphans, IterContent)

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
# Utility functions
#------------------------------------------------------------------------------

cdef inline bint list_contains(list objects, object item):
    """ Returns a Boolean integer indictating whether a list contains
    a given PyObject*. Note that this function does not perform rich
    comparison checking. It simply checking for pointer equality.

    """
    cdef bint res = False

    for obj in objects:
        if obj is item:
            res = True
            break

    return res 


cdef list remove_list_duplicates(list objects):
    """ Uniquify a list of objects. This function casts the list 
    to a set, the build a new list from the set in the same order
    as the original list. In terms of speed, this function only 
    pays off if the list contains ~1000 or more objects instead of
    simply performing a linear search before adding and object 
    to the list.

    """
    cdef set set_objects = set()
    cdef list new_objects = list()
    
    for obj in objects:
        if obj not in set_objects:
            new_objects.append(obj)
            set_objects.add(obj)
    
    return new_objects


cdef cycle_detect(DAGNode node):
    """ Detect a cycle in the graph starting at `node`.

    Performs a depth first search starting at `node` which
    continues until the search terminates or the search 
    revisits the starting node, thus indicating a cycle.

    Parameters
    ----------
    node : DAGNode instance
        The node to begin the cycle search.

    Returns
    -------
    out : tuple or None
        If a cycle is detected, the tuple will contain the 
        content of the start node of the search and the content
        of the node visited just before the completion of the 
        cycle (i.e. revisiting the start node). Otherwise, 
        returns None.

    """
    cdef Stack stack = Stack(2048)
    cdef DAGNode curr
    cdef DAGNode last

    last = node
    for child in node.children:
        stack.push(child)

    res = None
    while not stack.empty():
        curr = <DAGNode>stack.pop()
        if curr is node:
            res = (node.content, last.content)
            break
        for child in curr.children:
            stack.push(child)

    return res

           
#------------------------------------------------------------------------------
# Node Type
#------------------------------------------------------------------------------

cdef class DAGNode:
    """ This class represents a node on the DAGraph. 
    
    It carries a pointer to the node's contents, which must be a 
    hashable Python object, and two lists containing references to 
    parent and child DAGNode instances. This type is intended for 
    use solely by the DAGraph class, though there are methods
    which expose the internals.
    
    """
    def __cinit__(self, content):
        self.content = content
        self.children = list()
        self.parents = list() 
        self._check_parents = True
        self._check_children = True

    def get_content(self):
        """ Returns the Python object contained in this node. You
        should normally not be interacting with the DAGNodes directly.
        You can potentially break things by modifying the DAGNode.
        Use this method at your own risk.

        """
        return self.content

    def get_children(self):
        """ Returns the list of children DAGNodes for this node. You
        should normally not be interacting with the DAGNodes directly.
        You can potentially break things by modifying the DAGNode.
        Use this method at your own risk.

        """
        return self.children

    def get_parents(self):
        """ Returns the list of parent DAGNodes for this node. You
        should normally not be interacting with the DAGNodes directly.
        You can potentially break things by modifying the DAGNode.
        Use this method at your own risk.

        """
        return self.parents

    def set_content(self, content):
        """ Modify the content of this DAGNode in-place. You should
        normally not be interacting with the DAGNodes directly.
        You can potentially break things by modyifying the DAGNode.
        Use this method at your own risk.

        """
        self.content = content

    def set_children(self, list children):
        """ Set list of children DAGNodes for this node. You should
        normally not be interacting with the DAGNodes directly.
        You can potentially break things by modyifying the DAGNode.
        Use this method at your own risk.

        """
        self.children = children

    def set_parents(self, list, parents):
        """ Set list of parent DAGNodes for this node. You should
        normally not be interacting with the DAGNodes directly.
        You can potentially break things by modyifying the DAGNode.
        Use this method at your own risk.

        """
        self.parents = parents

    cdef inline bint has_children(self):
        """ Returns a Boolean integer indictating whether or not 
        the node has children.

        """
        return (<Py_ssize_t>len(self.children)) > 0

    cdef inline bint has_parents(self):
        """ Returns a Boolean integer indicating whether or not
        the node has parents.

        """
        return (<Py_ssize_t>len(self.parents)) > 0

    cdef inline void check_children(self, bint val):
        """ Set whether or not to check the list of children for 
        duplicates when adding a new child. When set to True, the 
        list of children will be immediately checked and cleaned 
        of all duplicates.

        """
        self._check_children = val
        if val:
            self.children = remove_list_duplicates(self.children)
        
    cdef inline void check_parents(self, bint val):
        """ Set whether or not to check the list of parents for 
        duplicates when adding a new parent. When set to True, the 
        list of parents will be immediately checked and cleaned 
        of all duplicates.

        """
        self._check_parents = val
        if val:
            self.parents = remove_list_duplicates(self.parents)

    cdef void add_parent(self, DAGNode parent):
        """ Add a parent node to this node. If the node is already 
        a parent of this node, and parent checking is turned on,
        then this method has no effect.

        """
        if self._check_parents:
            if list_contains(self.parents, parent):
                pass
            else:
                self.parents.append(parent)
        else:
            self.parents.append(parent)

    cdef void add_child(self, DAGNode child):
        """ Add a child node to this node. If the node is already
        a child of this node, and children checking is turned on,
        then this method has no effect.

        """
        if self._check_children:
            if list_contains(self.children, child):
                pass
            else:
                self.children.append(child)
        else:
            self.children.append(child)

    cdef void remove_parent(self, DAGNode parent):
        """ Remove a parent node from this node. If the node is not
        currently a parent of this node, then this method has no effect.

        """
        if list_contains(self.parents, parent):
            self.parents.remove(parent)

    cdef void remove_child(self, DAGNode child):
        """ Remove a child node from this node. If the node is not
        currently a child of this node, then this method has no effect.

        """
        if list_contains(self.children, child):
            self.children.remove(child)

#------------------------------------------------------------------------------
# DAG type
#------------------------------------------------------------------------------

cdef class DAGraph(object):
    """ A Directed Acyclic Graph.
    
    """
    def __cinit__(self):
        self._graph_nodes = {}
        self._cycle_detect = True

    def __iter__(self):
        """ Returns an iterator that will yield the content referenced
        by every node in the graph, in no particular order.

        """
        return self._graph_nodes.iterkeys()

    def __contains__(self, content):
        """ Returns whether or not the graph contains a DAGNode which
        contains the python object `content`.

        """
        return self.contains_fast(content)
   
    def __len__(self):
        """ Returns the number of nodes in the graph.

        """
        return len(self._graph_nodes)

    def contains(self, content):
        """ Return whether the graph contains a DAGNode which 
        contains the python object `content`.

        """
        return self.contains_fast(content)
  
    def get_node(self, content):
        """ Return the DAGNode instance which contains `content`.
        Will raise a KeyError if the object is not referenced 
        by any node.

        """
        return self.get_node_fast(content)

    def cycle_detct(self, bint val):
        """ Turn on/off the graph's cycle detector.

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
        self.cycle_detect_fast(val)

    def add_node(self, content):
        """ Add a node to the graph. 

        Add a node to the graph which will hold a (non-weak) reference
        to `content`. If `content` is already referenced by the graph, this 
        function has no effect. This function will raise a GraphError 
        if `content` is not hashable.

        Parameters
        ----------
        content : hashable object
            The created graph node will contain a non-weak reference
            to this object.

        Returns
        -------
        None

        """
        self.add_node_fast(content)

    def delete_node(self, content):
        """ Delete a node from the graph.

        If the graph contains a DAGNode which references `content`, 
        delete that node and any edges to that node; otherwise, 
        do nothing.

        Parameters
        ----------
        content : hashable object
            The object referenced by a DAGNode.

        Returns
        -------
        None

        """
        self.delete_node_fast(content)

    def add_edge(self, parent, child):
        """ Add an edge between two nodes.

        Connect two nodes in the graph by creating an edge from
        `parent` pointing towards `child`. If a given node does
        not exist in the graph, it will be added. If cycle detection
        is turned on, and adding this edge creates a cycle, then
        a CycleError will be raised, but the edge will still be
        added.

        Parameters
        ----------
        parent : hashable object
            The parent content.
        child : hashable object
            The child content.

        Returns
        -------
        None

        """
        self.add_edge_fast(parent, child)

    def add_edges_parents(self, tuple parents, child):
        """ Add an edge between many-to-one nodes.

        Connect nodes in the graph by creating an edge from each
        node in `parents` pointing towards `child`. If a given 
        node does not exist in the graph, it will be added. If cycle 
        detection is turned on, and adding any of these edges creates 
        a cycle, then a CycleError will be raised, but the edges will 
        still be added.

        Parameters
        ----------
        parents : tuple of hashable objects
            The parent content objects.
        child : hashable object
            The child content object.

        Returns
        -------
        None

        """
        self.add_edges_parents_fast(parents, child)

    def add_edges_children(self, parent, tuple children):
        """ Add an edge between one-to-many nodes.

        Connect nodes in the graph by creating an edge from `parent`
        pointing towards each node in `children`. If a given node does 
        not exist in the graph, it will be added. If cycle detection 
        is turned on, and adding any of these edges creates a cycle, 
        then a CycleError will be raised, but the edges will still be 
        added. 
        
        Parameters
        ----------
        parent : hashable object
            The parent content.
        children : tuple of hashable objects
            The child content objects.

        Returns
        -------
        None

        """
        self.add_edges_children_fast(parent, children)

    def remove_edge(self, parent, child):
        """ Remove an edge between two nodes.

        Disconnect two nodes in the graph by removing the edge point
        from `parent` towards `child`. If any of the given nodes do 
        not exist in the graph, or if the specified edge does not exist,
        the function has no effect.

        Parameters
        ----------
        source : hashable object
            The parent content.
        target : hashable object
            The child content.

        Returns
        -------
        None

        """
        self.remove_edge_fast(parent, child)

    def remove_edges_parents(self, tuple parents, child):
        """ Remove many-to-one edges.

        Disconnect nodes in the graph by removing the many-to-one 
        edges between `parents` and `child`. If any of the given 
        nodes do not exist in the graph, the function has no effect.
        If a specified edge does not exist, then it is ignored.

        Parameters
        ----------
        sources : tuple of hashable objects
            The parent content objects.
        target : hashable object
            The child content.

        Returns
        -------
        None

        """
        self.remove_edges_parents_fast(parents, child)

    def remove_edges_children(self, parent, tuple children):
        """ Remove one-to-many edges.

        Disconnect nodes in the graph by removing the one-to-many 
        edges between `source` and `targets`. If any of the given 
        nodes do not exist in the graph, the function has no effect.
        If a specified edge does not exist, then it is ignored.

        Parameters
        ----------
        source : hashable object
            The parent content.
        target : iterable of hashable objects
            The child content objects.

        Returns
        -------
        None

        """
        self.remove_edges_children_fast(parent, children)

    def reverse(self):
        """ Reverse the graph in-place.

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
        self.reverse_fast()

    def parentless(self):
        """ Returns an iterator which returns the content held in 
        nodes which have no parents.

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
        return self.parentless_fast()

    def childless(self):
        """ Returns an iterator which returns the content held in 
        nodes which have no children.

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
        return self.childless_fast()

    def orphans(self):
        """ Returns an iterator which returns the content held in
        nodes which have no parents or children.

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
        return self.orphans_fast()

    def children(self, content):
        """ Return the children of a given node.

        The children of a node are those nodes directly below 
        the given node in the graph. If the given node is not 
        contained in the graph, a GraphError is raised.

        Parameters
        ----------
        content : hashable object
            The parent content.

        Returns
        -------
        out : list
            The children of the given node. Will be empty if the
            node has no children.

        """
        return self.children_fast(content)

    def parents(self, content):
        """ Return the parents of a given node.

        The parents of a node are those nodes directly above 
        the given node in the graph. A node may have more than 
        one parent. If the given node is not contained in the 
        graph, a GraphError is raised.

        Parameters
        ----------
        content : hashable object
            The child content.

        Returns
        -------
        out : list
            The parents of the given node. Will be empty if the
            node has no parents.

        """
        return self.parents_fast(content)

    def traverse(self, content, bint descend=True, bint dfs=True, bint level=False):
        """ Returns an iterator that performs a search through the graph.

        Returns an iterator that returns nodes in the graph by walking
        the graph starting at `content` and traversering the graph 
        according to the specified arguments.
        
        If `content` does not exist in the graph, a GraphError will be raised.

        Parameters
        ----------
        content : hashable object
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

        """
        return self.traverse_fast(content, descend, dfs, level)
    
    #------------------------------------------------------------------
    # C implementation methods
    #------------------------------------------------------------------
    cdef inline bint contains_fast(self, content):
        cdef bint res = False

        if content in self._graph_nodes:
            res = True

        return res
   
    cdef inline DAGNode get_node_fast(self, content):
        return <DAGNode>self._graph_nodes[content]

    cdef inline cycle_detect_fast(self, bint val):
        self._cycle_detect = val

    cdef add_node_fast(self, content):
        if not self.contains_fast(content):
            node = DAGNode(content)
            self._graph_nodes[content] = node

    cdef delete_node_fast(self, content):
        cdef DAGNode node

        if self.contains_fast(content): 
            node = self.get_node_fast(content)
        
            for parent in node.parents:
                (<DAGNode>parent).remove_child(node)

            for child in node.children:
                (<DAGNode>child).remove_parent(node)

            del self._graph_nodes[content]

    cdef add_edge_fast(self, parent, child):
        self.add_edges_children_fast(parent, (child,))

    cdef add_edges_parents_fast(self, tuple parents, child):
        cdef bint disable_check_parents
        cdef DAGNode parent_node, child_node

        if not self.contains_fast(child):
            self.add_node_fast(child) 

        child_node = self.get_node_fast(child)
        disable_check_parents = (<Py_ssize_t>len(parents)) >= 1000

        if disable_check_parents:
            # 1000 elements is approx. the number necessary such that
            # checking for duplicates with a set operation results
            # in a gain, vs a linear search through the parent nodes 
            # upon each addition.
            child_node.check_parents(False)

        for parent in parents:
            if not self.contains_fast(parent):
                self.add_node_fast(parent)
            parent_node = self.get_node_fast(parent)
            child_node.add_parent(parent_node)
            parent_node.add_child(child_node)
        
        if disable_check_parents:
            child_node.check_parents(True)

        if self._cycle_detect:
            cycles = []
            for parent in parents:
                parent_node = self.get_node_fast(parent)
                cycle = cycle_detect(parent_node)
                if cycle is not None:
                    cycles.append(cycle)
            if cycles:
                raise CycleError(cycles)

    cdef add_edges_children_fast(self, parent, tuple children):
        cdef bint disable_check_children
        cdef DAGNode parent_node, child_node

        if not self.contains_fast(parent):
            self.add_node_fast(parent) 
        
        parent_node = self.get_node_fast(parent)
        disable_check_children = (<Py_ssize_t>len(children)) >= 1000

        if disable_check_children:
            # 1000 elements is approx. the number necessary such that
            # checking for duplicates with set operations results
            # in a gain, vs a linear search through the children nodes
            # upon each addition.
            parent_node.check_children(False)

        for child in children:
            if not self.contains_fast(child):
                self.add_node_fast(child)
            child_node = self.get_node_fast(child)
            child_node.add_parent(parent_node)
            parent_node.add_child(child_node)

        if disable_check_children:
            # re-enabling duplicate checking will uniqify the 
            # the child list
            parent_node.check_children(True)

        if self._cycle_detect:
            cycle = cycle_detect(parent_node)
            if cycle:
                raise CycleError(cycle)

    cdef remove_edge_fast(self, parent, child):
        self.remove_edges_children_fast(parent, (child,))

    cdef remove_edges_parents_fast(self, tuple parents, child):
        cdef DAGNode parent_node, child_node

        if not self.contains_fast(child):
            return
        
        child_node = self.get_node_fast(child)
        for parent in parents:
            if not self.contains_fast(parent):
                continue
            parent_node = self.get_node_fast(parent)
            child_node.remove_parent(parent_node)
            parent_node.remove_child(child_node)

    cdef remove_edges_children_fast(self, parent, tuple children):
        cdef DAGNode parent_node, child_node

        if not self.contains_fast(parent):
            return

        parent_node = self.get_node_fast(parent)
        for child in children:
            if not self.contains_fast(child):
                continue
            child_node = self.get_node_fast(child)
            child_node.remove_parent(parent_node)
            parent_node.remove_child(child_node)

    cdef reverse_fast(self):
        for node in self._graph_nodes.itervalues():
            cnode = <DAGNode>node
            cnode.children, cnode.parents = cnode.parents, cnode.children

    cdef parentless_fast(self):
        return IterParentless(self._graph_nodes.itervalues())

    cdef childless_fast(self):
        return IterChildless(self._graph_nodes.itervalues())

    cdef orphans_fast(self):
        return IterOrphans(self._graph_nodes.itervalues())

    cdef children_fast(self, content):
        cdef DAGNode node

        if not self.contains_fast(content):
            raise GraphError("Content `%s` does not exist in the graph." % content)

        node = self.get_node_fast(content)
        return IterContent(iter(node.children))

    cdef parents_fast(self, content):
        cdef DAGNode node

        if not self.contains_fast(content):
            raise GraphError("Content `%s` does not exist in the graph." % content)
        
        node = self.get_node_fast(content)
        return IterContent(iter(node.parents))

    cdef traverse_fast(self, content, bint descend=True, bint dfs=True, bint level=False):
        cdef DAGNode node

        if not self.contains_fast(content):
            raise GraphError("Content `%s` does not exist in the graph." % content)

        node = self.get_node_fast(content)

        if descend:
            if dfs:
                if level:
                    return IterDFSDownLevel(node)
                else:
                    return IterDFSDown(node)
            else:
                if level:
                    return IterBFSDownLevel(node)
                else:
                    return IterBFSDown(node)
        else:
            if dfs:
                if level:
                    return IterDFSUpLevel(node)
                else:
                    return IterDFSUp(node)
            else:
                if level:
                    return IterBFSUpLevel(node)
                else:
                    return IterBFSUp(node)

