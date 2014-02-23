#!/usr/bin/env python
"""
`sqlamp` benchmarks.
"""
from time import time
import os
import unittest

import sqlalchemy
import sqlamp

import tests._testlib as _testlib
_testlib.setup()
from tests._testlib import Cls, make_session, tbl


class BenchmarkTestCase(_testlib._BaseTestCase):
    def _base_insertion_benchmark(self, num_nodes, num_roots, commit_once):
        import random
        MAX_DEPTH = 20
        node_ids = [list() for x in range(MAX_DEPTH)]
        def random_parent_id():
            depth = MAX_DEPTH * 1.5625 * (random.random() - 0.2) ** 2
            depth = int(depth)
            while not node_ids[depth]:
                depth -= 1
            return random.choice(node_ids[depth])

        start = time()
        for x in range(num_roots):
            root = Cls()
            self.sess.add(root)
            self.sess.flush()
            self.sess.commit()
            node_ids[0].append(root.id)
        for x in range(num_nodes - num_roots):
            parent_id = random_parent_id()
            node = Cls(parent_id=parent_id)
            self.sess.add(node)
            self.sess.flush()
            if not commit_once:
                self.sess.commit()
            if node.mp_depth >= MAX_DEPTH:
                pass
            else:
                node_ids[node.mp_depth].append(node.id)
        if commit_once:
            self.sess.commit()

        elapsed = time() - start
        transactions = commit_once and "all in one transaction" \
                                   or "each in self transaction"
        print("%d insertions in %.2f seconds %s " \
              "(%.2f insertions per second)" % \
              (num_nodes, elapsed, transactions, num_nodes / elapsed))

    def _descendants_benchmark(self, num_passes):
        total_children = 0
        total_nodes = self.sess.query(Cls).count()
        start = time()
        for x in range(num_passes):
            for node in self.sess.query(Cls):
                total_children += len(node.mp.query_descendants().all())
        elapsed = time() - start
        queries = num_passes * total_nodes
        average_children = float(total_children) / queries
        print("%d queries in %.2f seconds (%.2f queries per second), " \
              "average of %.2f descendants in each node." % \
              (queries, elapsed, queries / elapsed, average_children))

    def test_benchmark(self):
        if not os.environ.get('BENCHMARK'):
            print("NB: benchmarks skipped.")
            return
        print("BENCHMARKING...")
        self._base_insertion_benchmark(
            commit_once=False, num_nodes=1000, num_roots=10
        )
        self._base_insertion_benchmark(
            commit_once=True, num_nodes=1000, num_roots=10
        )
        self._descendants_benchmark(num_passes=2)


def get_suite():
    return unittest.TestLoader().loadTestsFromTestCase(BenchmarkTestCase)


if __name__ == '__main__':
    import os
    os.environ['BENCHMARK'] = '1'
    unittest.TextTestRunner(verbosity=2).run(get_suite())

