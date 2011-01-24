cimport cpython as cpy


#------------------------------------------------------------------------------
# Stack
#------------------------------------------------------------------------------

cdef class Stack:

    cdef cpy.PyObject** _stack  # The underlying C-Array of PyObject*
    cdef size_t _top            # The idx where to put the next push
    cdef size_t _capacity       # The number of elements the stack can hold
    cdef size_t _max_capacity   # The maximum size the stack is allowed to grow

    cdef _realloc(self)

    cdef inline bint empty(self)
    cdef inline bint full(self)
    cdef inline size_t size(self)
    cdef inline size_t capacity(self)
    
    cdef push(self, object)
    cdef pop(self)

#------------------------------------------------------------------------------
# Queue
#------------------------------------------------------------------------------

cdef class Queue:

    cdef cpy.PyObject** _queue  # The underlying C-Array of PyObject*
    cdef size_t _capacity       # The number of elements the queue can hold
    cdef size_t _max_capacity   # The maximum size to which the queue can grow
    cdef size_t _front          # The idx where the next element is popped
    cdef size_t _rear           # The idx where the next element is pushed
    cdef size_t _n_elements     # The number of elements in the queue
    
    cdef _realloc(self)

    cdef inline bint empty(self)
    cdef inline bint full(self)
    cdef inline size_t size(self)
    cdef inline size_t capacity(self)

    cdef push(self, object)
    cdef pop(self)
       


