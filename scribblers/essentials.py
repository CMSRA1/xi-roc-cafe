# Tai Sakuma <tai.sakuma@cern.ch>
import numpy as np

##__________________________________________________________________||
class FuncOnNumpyArrays(object):
    def __init__(self, src_arrays, out_name, func):
        self.src_arrays = src_arrays
        self.out_name = out_name
        self.func = func

    def __repr__(self):
        name_value_pairs = (
            ('src_arrays', self.src_arrays),
            ('out_name', self.out_name),
            ('func', self.func),
        )
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(['{} = {!r}'.format(n, v) for n, v in name_value_pairs]),
        )

    def begin(self, event):
        self.out = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        setattr(event, self.out_name, self.out)

    def event(self, event):
        self._attach_to_event(event)
        self.out[:] = self.func(*[np.array(getattr(event, n)) for n in self.src_arrays])

##__________________________________________________________________||
