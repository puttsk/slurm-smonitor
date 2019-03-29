# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

class NodeSpecification(object):
    def __init__(self, sockets, cpus_per_socket):
        self.sockets = sockets
        self.cpus_per_socket = cpus_per_socket
    
    @property
    def cpus(self):
        return self.sockets * self.cpus_per_socket

class NodeInformation(object):
    def __init__(self, spec):
        if type(spec) is not NodeSpecification:
            raise TypeError('spec must be a NodeSpecification instance')
        
        self._spec = spec
        self._cpu_set = [0x0]*self._spec.sockets
    
    @property
    def specification(self):
        return self._spec
    
    @property
    def cpus_per_socket(self):
        return self._spec.cpus_per_socket

    @property
    def sockets(self):
        return self._spec.sockets

    @specification.setter
    def specification(self, val):
        if type(spec) is not NodeSpecification:
            raise TypeError('specification must be a NodeSpecification instance')
        self._spec = spec

    def set_cpus(self, cpu_range):
        self._cpu_usage = [0x0]*self._spec.sockets

        if type(cpu_range) is str:
            cpu_range = [int(x) for x in cpu_range.split('-')]

        if len(cpu_range) == 1:
            cpu_range = [cpu_range[0], cpu_range[0]]

        for socket in range(self.specification.sockets):
            if cpu_range[0] >= (socket+1) * self.cpus_per_socket:
                # the CPU range belongs to other sockets
                continue

            if cpu_range[1] < socket * self.cpus_per_socket:
                # the CPU range belongs to other sockets
                break
            
            begin = max(cpu_range[0], socket * self.cpus_per_socket)
            end = min(cpu_range[1], (socket+1) * self.cpus_per_socket - 1)

            cpu_init_id = socket * self.cpus_per_socket

            begin_mask = (1 << (begin - cpu_init_id)) - 1
            end_mask = (1 << (end - cpu_init_id+ 1)) - 1

            cpus_mask = end_mask ^ begin_mask

            self._cpu_usage[socket] = cpus_mask

    def __str__(self):
        return ' '.join(["{:0{length}b}".format(x, length=self.cpus_per_socket)[::-1] for x in self._cpu_usage])
        
    def __repr__(self):
        return str(self)

    def bitmask_str(self, delimiter=' ', max_length=40):
        if self.cpus_per_socket * self.sockets <= max_length:
            return delimiter.join(["{:0{length}b}".format(x, length=self.cpus_per_socket)[::-1] for x in self._cpu_usage])
        else:
            return delimiter.join(["{:0{length}X}".format(x, length=self.cpus_per_socket/4)[::-1] for x in self._cpu_usage])
