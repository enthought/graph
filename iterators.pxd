from c_collections cimport Stack, Queue


#------------------------------------------------------------------------------
# Iterators
#------------------------------------------------------------------------------

cdef class IterDFSDown(object):

    cdef Stack _stack


cdef class IterDFSUp(object):

    cdef Stack _stack


cdef class IterBFSDown(object):

    cdef Queue _queue


cdef class IterBFSUp(object):
    
    cdef Queue _queue


cdef class IterDFSDownLevel(object):

    cdef Stack _stack


cdef class IterDFSUpLevel(object):

    cdef Stack _stack


cdef class IterBFSDownLevel(object):

    cdef Queue _queue


cdef class IterBFSUpLevel(object):
    
    cdef Queue _queue


cdef class IterParentless(object):

    cdef object node_iterator


cdef class IterChildless(object):
    
    cdef object node_iterator


cdef class IterOrphans(object):
    
    cdef object node_iterator


cdef class IterContent(object):

    cdef object node_iterator



