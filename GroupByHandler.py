import sys
import pickle
import heapq

from collections import defaultdict


class GroupByHandler:
    """Class that groups input tuples by key.

    Attributes:
      memory_limit (int): the maximum amount of memory this process should use, in bytes
    """
    count = 0

    def __init__(self, memory_limit):
        self.cur_memory_used = 0
        self.memory_limit = memory_limit
        self.groups = defaultdict(list)

    def group_by(self, input):
        """The groupby entry point

        Args:
          input: an iterator of string two-tuples. For example, `iter([('a', 'y'), ('b', 'c'), ('a', 'z')])`

        Returns:
          an iterator of (string, list of string) tuples. For the above example input, returns
          `iter([ ('a', ['y', 'z']), ('b', ['c']) ])`
        """
        # ADD YOUR CODE HERE.
        while True:
            try:
                key_value = next(input)
                self.cur_memory_used += sys.getsizeof(key_value)
                if not self.cur_memory_used < self.memory_limit:
                    self.groups[key_value[0]].append(key_value[1])
                else:
                    f_group = open('group_{}'.format(GroupByHandler.count), "w")
                    f_group.write(pickle.dump(self.groups))
                    f_group.close()
                    self.groups = defaultdict(list)
                    self.cur_memory_used = 0
                    GroupByHandler.count += 1
            except StopIteration:
                break

    files = open('group_0.txt'), open('group_1.txt')
    with open('merged.txt', 'w') as out:
        f_streams = (pickle.load(f) for f in files)
        f_stream = heapq.merge(*f_streams)
        out.write(pickle.dump(f_stream))


        # def group_by(self, key_value_iter):

        #
        #     for key,value in self.groups.items():
        #         yield (key,value)
