cimport cpython as cpy

from c_collections cimport Stack, Queue
from dagraph cimport DAGNode


#------------------------------------------------------------------------------
# Iterators
#------------------------------------------------------------------------------

cdef class IterDFSDown(object):
    """ A depth first iterator that traverses the DAGraph downward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Stack stack = Stack(512)

        for child in start_node.children:
            stack.push(child)

        self._stack = stack

    def __iter__(self):
        return self

    def __next__(self):
        cdef Stack stack = self._stack
        cdef DAGNode curr

        if stack.empty():
            raise StopIteration

        curr = <DAGNode>stack.pop()
        for child in curr.children:
            stack.push(child)

        return curr.content


cdef class IterDFSUp(object):
    """ A depth first iterator that traverses the DAGraph upward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Stack stack = Stack(512)

        for parent in start_node.parents:
            stack.push(parent)

        self._stack = stack

    def __iter__(self):
        return self

    def __next__(self):
        cdef Stack stack = self._stack
        cdef DAGNode curr

        if stack.empty():
            raise StopIteration

        curr = <DAGNode>stack.pop()
        for parent in curr.parents:
            stack.push(parent)

        return curr.content

 
cdef class IterBFSDown(object):
    """ A breadth first iterator that traverses the DAGraph downward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Queue queue = Queue(512)
        
        for child in start_node.children:
            queue.push(child)

        self._queue = queue

    def __iter__(self):
        return self

    def __next__(self):
        cdef Queue queue = self._queue
        cdef DAGNode curr

        if queue.empty():
            raise StopIteration

        curr = <DAGNode>queue.pop()
        for child in curr.children:
            queue.push(child)

        return curr.content
 

cdef class IterBFSUp(object):
    """ A breadth first iterator that traverses the DAGraph upward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Queue queue = Queue(512)

        for parent in start_node.parents:
            queue.push(parent)

        self._queue = queue

    def __iter__(self):
        return self

    def __next__(self):
        cdef Queue queue = self._queue
        cdef DAGNode curr

        if queue.empty():
            raise StopIteration

        curr = <DAGNode>queue.pop()
        for parent in curr.parents:
            queue.push(parent)

        return curr.content


cdef class IterDFSDownLevel(object):
    """ A depth first iterator that traverses the DAGraph downward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Stack stack = Stack(512)

        for child in start_node.children:
            stack.push((child, 1))

        self._stack = stack

    def __iter__(self):
        return self

    def __next__(self):
        cdef Stack stack = self._stack
        cdef DAGNode curr
        cdef long curr_level
        cdef long new_level

        if stack.empty():
            raise StopIteration

        py_node, py_level = <tuple>stack.pop()
        curr = <DAGNode>py_node
        curr_level = <long>py_level
        new_level = curr_level + 1

        for child in curr.children:
            stack.push((child, new_level))

        return (curr.content, py_level)


cdef class IterDFSUpLevel(object):
    """ A depth first iterator that traverses the DAGraph upward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Stack stack = Stack(512)

        for parent in start_node.parents:
            stack.push((parent, 1))

        self._stack = stack

    def __iter__(self):
        return self

    def __next__(self):
        cdef Stack stack = self._stack
        cdef DAGNode curr
        cdef long curr_level
        cdef long new_level

        if stack.empty():
            raise StopIteration

        py_node, py_level = <tuple>stack.pop()
        curr = <DAGNode>py_node
        curr_level = <long>py_level
        new_level = curr_level + 1

        for parent in curr.parents:
            stack.push((parent, new_level))

        return (curr.content, py_level)

 
cdef class IterBFSDownLevel(object):
    """ A breadth first iterator that traverses the DAGraph downward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Queue queue = Queue(512)
        
        for child in start_node.children:
            queue.push((child, 1))

        self._queue = queue

    def __iter__(self):
        return self

    def __next__(self):
        cdef Queue queue = self._queue
        cdef DAGNode curr
        cdef long curr_level
        cdef long new_level

        if queue.empty():
            raise StopIteration
        
        py_node, py_level = <tuple>queue.pop()
        curr = <DAGNode>py_node
        curr_level = <long>py_level
        new_level = curr_level + 1

        for child in curr.children:
            queue.push((child, new_level))

        return (curr.content, py_level)
 

cdef class IterBFSUpLevel(object):
    """ A breadth first iterator that traverses the DAGraph upward
    from a given node. 
    
    """
    def __cinit__(self, DAGNode start_node):
        cdef Queue queue = Queue(512)

        for parent in start_node.parents:
            queue.push((parent, 1))

        self._queue = queue

    def __iter__(self):
        return self

    def __next__(self):
        cdef Queue queue = self._queue
        cdef DAGNode curr
        cdef long curr_level
        cdef long new_level

        if queue.empty():
            raise StopIteration

        py_node, py_level = <tuple>queue.pop()
        curr = <DAGNode>py_node
        curr_level = <long>py_level
        new_level = curr_level + 1

        for parent in curr.parents:
            queue.push((parent, new_level))

        return (curr.content, py_level)


cdef class IterParentless(object):
    """ An iterator which returns the content of nodes which
    have no parents.

    """
    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <DAGNode>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents() and cnode.has_children():
                return cnode.content


cdef class IterChildless(object):
    """ An iterator which returns the content of nodes which
    have no children.
    
    """
    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <DAGNode>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if cnode.has_parents() and not cnode.has_children():
                return cnode.content


cdef class IterOrphans(object):
    """ An iterator which returns the content of nodes which
    have no parents or children.

    """
    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            cnode = <DAGNode>cpy.PyIter_Next(self.node_iterator)
            if <void*>cnode == NULL:
                raise StopIteration

            if not cnode.has_parents() and not cnode.has_children():
                return cnode.content


cdef class IterContent(object):
    """ An iterator which returns the contents of nodes.

    """
    def __cinit__(self, iterator):
        if not cpy.PyIter_Check(iterator):
            raise TypeError('Argument must be an iterator.')
        
        self.node_iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        cnode = <DAGNode>cpy.PyIter_Next(self.node_iterator)
        if <void*>cnode == NULL:
            raise StopIteration
  
        return cnode.content


