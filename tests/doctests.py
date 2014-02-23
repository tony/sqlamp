#!/usr/bin/env python
"""
`sqlamp` tests which is simpler to implement and revise as doctests.

    >>> from tests._testlib import Cls, make_session, tbl

Testing exceptions on performing queries on objects that are not
persist in session:

    >>> session = make_session(autoflush=True)
    >>> root = Cls(name='root')
    >>> session.add(root)
    >>> session.flush()

    >>> node = Cls(name='node', parent=root)
    >>> node.mp.query_children() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' state. put it in the session to be able to get filters and perform queries.
    >>> node.mp.query_ancestors() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...
    >>> node.mp.query_descendants() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...
    >>> node.mp.filter_children() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...
    >>> node.mp.filter_ancestors() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...
    >>> node.mp.filter_descendants() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...
    >>> node.mp.filter_parent() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node...> seems to be in 'transient' ...

Now add node to the session:

    >>> session.add(node)

Performing queries should work now, even if we didn't called explicitly
`session.flush()`. First query will call it automatically, because
object's session has 'autoflush' option set to `True`. That's it, in
the first call of `node.mp.query_*()` or `node.mp.filter_*()` if node is
in 'pending' state node's session flushes:

    >>> node.mp.query_ancestors().all() # doctest: +ELLIPSIS
    [<Cls root...>]
    >>> bool(session.dirty or session.new)
    False

Put node to 'detached' state and try to use queries again:

    >>> session.expunge(node)
    >>> node.mp.query_ancestors() # doctest: +ELLIPSIS
    <sqlalchemy.orm.query.Query object at 0x...>

Note that we get query objects now but they are not bound to a sessions,
so can not be used directly:

    >>> node.mp.query_ancestors().session is None
    True
    >>> node.mp.query_descendants() # doctest: +ELLIPSIS
    <sqlalchemy.orm.query.Query object at 0x...>
    >>> node.mp.query_children() # doctest: +ELLIPSIS
    <sqlalchemy.orm.query.Query object at 0x...>

But now we should be able to get filters, as `node` is now in 'detached'
state and its fields 'path', 'depth' and 'tree_id' has real values:

    >>> str(node.mp.filter_children()) # doctest: +ELLIPSIS
    'tbl.mp_tree_id = ... AND tbl.mp_path > ... AND tbl.mp_path < ... AND tbl.mp_depth = ...'
    >>> str(node.mp.filter_ancestors()) # doctest: +ELLIPSIS
    'tbl.mp_tree_id = ... AND ... LIKE ...tbl.mp_path... AND tbl.mp_depth < ...'
    >>> str(node.mp.filter_descendants()) # doctest: +ELLIPSIS
    'tbl.mp_tree_id = ... AND tbl.mp_path > ... AND tbl.mp_path < ...'
    >>> str(node.mp.filter_parent()) # doctest: +ELLIPSIS
    'tbl.id = ...'

Now test queries on 'pending' node stored in session with autoflush
disabled. No query could be performed:

    >>> session.delete(node); session.commit(); session.close()
    >>> del session, node
    >>> session = make_session(autoflush=False)
    >>> node2 = Cls(name='node2')
    >>> session.add(node2)
    >>> node2.mp.query_children() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state and attached to non-autoflush session. call `session.flush()` to be able to get filters and perform queries.
    >>> node2.mp.query_ancestors() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...
    >>> node2.mp.query_descendants() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...
    >>> node2.mp.filter_ancestors() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...
    >>> node2.mp.filter_children() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...
    >>> node2.mp.filter_descendants() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...
    >>> node2.mp.filter_parent() # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: instance <Cls node2...> is in 'pending' state ...

Making sure that flush goes ok:

    >>> session.flush()
    >>> node2 in session
    True
    >>> node2 in session.dirty or node2 in session.new
    False
    >>> session.delete(node2); session.commit(); session.close()

"""
import doctest
import unittest

import sqlalchemy
import sqlamp

import tests._testlib as _testlib
_testlib.setup()
from tests._testlib import Cls, make_session, tbl


def get_suite():
    suite = doctest.DocTestSuite(sqlamp)
    suite.addTest(doctest.DocFileSuite(__file__, module_relative=False))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(get_suite())

