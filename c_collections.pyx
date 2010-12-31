cimport cpython as cpy
from libc.string cimport memcpy


#------------------------------------------------------------------------------
# Stack
#------------------------------------------------------------------------------

cdef class Stack:
    """ A C-Array based PyObject* stack. A stack is initialized with a single
    argument to the constructor which is the initial capacity of the stack.
    An optional argument may be given to the constructor which is the max size
    of the stack. This optional argument defaults to 0. Once the capacity of 
    the stack is reached, the stack will double in size upon the next push. 
    The stack will continue to grow until its max size is reached. If max
    size is 0, then the stack will grow bounded by the limit of size_t. The 
    stack is never shrunken. If the stack is full, and already at its maximum
    allowed size, then attempting to push more objects will raise a 
    RuntimeError. Popping from an empty stack will raise an IndexError.

    Example
    -------
    stack = Stack(128)
    stack.push(('spam',))
    stack.push(42)
    stack.push('ham')
    stack.push(np.array(['e', 'g', 'g', 's']))

    while not stack.empty():
        print stack.pop()

    """
    def __cinit__(self, size_t init_capacity, size_t max_capacity=0):
        cdef void* mem
       
        if init_capacity == 0:
            raise ValueError("Cannot create a stack of 0 initial capacity.")

        if max_capacity != 0:
            if max_capacity < init_capacity:
                raise ValueError("The max stack capacity is defined as less "
                                 "than the initial capacity.")
                                
        mem = cpy.PyMem_Malloc(sizeof(cpy.PyObject*) * init_capacity)
        if mem is NULL:
            raise MemoryError("Could not allocate stack of %s elements." % init_capacity)

        self._stack = <cpy.PyObject**>mem
        self._top = 0
        self._capacity = init_capacity
        self._max_capacity = max_capacity

    def __dealloc__(self):
        # The stack must be properly emptied before being garbage 
        # collected so that any PyObject* left in the stack are
        # properly decref'd
        while not self.empty():
            self.pop()
        cpy.PyMem_Free(<void*>self._stack)
    
    cdef _realloc(self):
        cdef size_t new_capacity
        cdef void* mem
       
        new_capacity = self._capacity * 2
        if self._max_capacity != 0:
            if self._capacity == self._max_capacity:
                raise RuntimeError("Maximum stack capacity reached.")

            if new_capacity > self._max_capacity:
                new_capacity = self._max_capacity

        # If new_capactity is less than self._capacity, then 
        # the size_t type has overflowed. If the stack ever 
        # gets that big, there is probably something wrong with 
        # the user's code, so we just raise an exception instead
        # of trying to do overly-clever work arounds.
        if new_capacity <= self._capacity:
            raise OverflowError("Maximum stack size exceeded.")

        mem = cpy.PyMem_Realloc(<void*>self._stack, sizeof(cpy.PyObject*) * new_capacity)
        if mem is NULL:
            raise MemoryError("Could not reallocate stack of %s elements" % new_capacity)

        self._stack = <cpy.PyObject**>mem
        self._capacity = new_capacity

    cdef inline bint empty(self):
        """ Returns a Boolean integer indicating whether or not the 
        stack is empty.

        """
        return self._top == 0
    
    cdef inline bint full(self):
        """ Returns a Boolean integer indicating whether or not the 
        stack is full.

        """
        return self._top == self._capacity

    cdef inline size_t size(self):
        """ Returns the number of elements contained in the stack.

        """
        return self._top

    cdef inline size_t capacity(self):
        """ Returns the number of elements the stack is capable of
        holding.

        """
        return self._capacity

    cdef push(self, content):
        """ Pushes an object onto the stack. This creates an owned
        reference the object.

        """
        if self.full():
            self._realloc()
        
        cpy.Py_INCREF(content)
        self._stack[self._top] = <cpy.PyObject*>content
        self._top += 1

    cdef pop(self):
        """ Pops and returns the top object on the stack. This releases
        an owned reference to the object.

        """
        if self.empty():
            raise IndexError('Pop from empty stack.')

        content = <object>self._stack[self._top - 1]
        cpy.Py_DECREF(content)
        self._top -= 1

        return content


#------------------------------------------------------------------------------
# Queue
#------------------------------------------------------------------------------

cdef class Queue:
    """ A C-Array based PyObject* queue. A queue is initialized with a single
    argument to the constructor which is the initial capacity of the queue.
    An optional argument may be given to the constructor which is the max size
    of the queue. This optional argument defaults to 0. Once the capacity of 
    the queue is reached, the queue will double in size upon the next push. 
    The queue will continue to grow until its max size is reached. If max
    size is 0, then the queue will grow bounded by the limit of size_t. The 
    queue is never shrunken. If the queue is full and already at its maximum
    allowed size, then attempting to push additional objects will raise a
    RuntimeError. Popping from an empty queue will raise an IndexError. 

    Example
    -------
    queue = Queue(128)
    queue.push(('spam',))
    queue.push(42)
    queue.push('ham')
    queue.push(np.array(['e', 'g', 'g', 's']))

    while not queue.empty():
        print queue.pop()

    """
    def __cinit__(self, size_t init_capacity, size_t max_capacity=0):
        cdef void* mem
       
        if init_capacity == 0:
            raise ValueError("Cannot create a queue of 0 initial capacity.")

        if max_capacity != 0:
            if max_capacity < init_capacity:
                raise ValueError("The max queue capacity is defined as less "
                                 "than the initial capacity.")
                                
        mem = cpy.PyMem_Malloc(sizeof(cpy.PyObject*) * init_capacity)
        if mem is NULL:
            raise MemoryError("Could not allocate stack of %s elements." % init_capacity)

        self._queue = <cpy.PyObject**>mem
        self._front = 0
        self._rear = 0
        self._n_elements = 0
        self._capacity = init_capacity
        self._max_capacity = max_capacity

    def __dealloc__(self):
        while not self.empty():
            self.pop()
        cpy.PyMem_Free(<void*>self._queue)

    cdef _realloc(self):
        cdef size_t new_capacity
        cdef void* mem
        cdef void* start_src
        cdef void* start_dst
        cdef size_t copy_size
       
        new_capacity = self._capacity * 2
        if self._max_capacity != 0:
            if self._capacity == self._max_capacity:
                raise RuntimeError("Maximum queue capacity reached.")

            if new_capacity > self._max_capacity:
                new_capacity = self._max_capacity

        # If new_capactity is less than self._capacity, then 
        # the size_t type has overflowed. If the queue ever 
        # gets that big, there is probably something wrong with 
        # the user's code, so we just raise an exception instead
        # of trying to do overly-clever work arounds.
        if new_capacity <= self._capacity:
            raise OverflowError("Maximum queue size exceeded.")

        # We do a malloc here instead of realloc because the queue
        # could currently be wrapped-around the end of the array.
        # Instead, we malloc new memory and memcpy the appropriate
        # parts, then release the old memory.
        mem = cpy.PyMem_Malloc(sizeof(cpy.PyObject*) * new_capacity)
        if mem is NULL:
            raise MemoryError("Could not reallocate stack of %s elements" % new_capacity)

        # If self._front < self._rear, then we only need to copy a 
        # continguous block of memory. i.e. the queue is not wrapped-around
        if self._front < self._rear:
            start_dst = mem
            start_src = <void*>(self._queue + self._front)
            copy_size = sizeof(cpy.PyObject*) * (self._rear - self._front)
            memcpy(start_dst, start_src, copy_size)
        
        # If self._front >= self._rear, then the queue is wrapped around
        # and we need to do two memcpy's
        else:
            start_dst = mem
            start_src = <void*>(self._queue + self._front)
            copy_size = sizeof(cpy.PyObject*) * (self._capacity - self._front)
            memcpy(mem, start_src, copy_size)

            start_dst = <void*>((<cpy.PyObject**>mem) + (self._capacity - self._front))
            start_src = <void*>self._queue
            copy_size = sizeof(cpy.PyObject*) * self._rear
            memcpy(start_dst, start_src, copy_size)

        # now we need to reset the front, rear, and capacity of the queue
        self._front = 0
        self._rear = self._n_elements
        self._capacity = new_capacity

        # finally, free the old memory and assign the new memory 
        # as the queue
        cpy.PyMem_Free(self._queue)
        self._queue = <cpy.PyObject**>mem
        self._capacity = new_capacity

    cdef inline bint empty(self):
        """ Returns a Boolean integer indictating whether or not the 
        queue is empty.

        """
        return self._n_elements == 0
    
    cdef inline bint full(self):
        """ Returns a Boolean integer indicating whether or not the
        queue is full.

        """
        return self._n_elements == self._capacity

    cdef inline size_t size(self):
        """ Returns the number of elements in the queue.

        """
        return self._n_elements

    cdef inline size_t capacity(self):
        """ Returns the number of elements the queue is capable of 
        holding.

        """
        return self._capacity

    cdef push(self, content):
        """ Push an object onto the rear of the queue. This creates
        an owned reference to the object.

        """
        if self.full():
            self._realloc()
        
        cpy.Py_INCREF(content)
        self._queue[self._rear] = <cpy.PyObject*>content

        self._n_elements += 1
        self._rear += 1

        # If self._rear is equal to self._capacity
        # then cycle around to the start of the array.
        if self._rear == self._capacity:
            self._rear = 0 

    cdef pop(self):
        """ Pop and return an object from the front of the queue.
        This releases an owned reference to the object.

        """
        if self.empty():
            raise IndexError("Pop from empty queue.")

        content = <object>self._queue[self._front]
        cpy.Py_DECREF(content)

        self._n_elements -= 1
        self._front += 1

        # If self._front is equal to self._capacity
        # then cycle around to the start of the array
        if self._front == self._capacity:
            self._front = 0

        return content

