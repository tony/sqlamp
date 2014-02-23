#!/usr/bin/env python
"""
`sqlamp` -- Materialized Path for SQLAlchemy
============================================

`sqlamp` is an implementation of an efficient algorithm for working
with hierarchical data structures -- `Materialized Path`. `sqlamp`
uses (and depends of) `SQLAlchemy <http://sqlalchemy.org>`_.

`Materialized Path` is a way to store (and fetch) a trees in a relational
databases. It is the compromise between `Nested Sets` and `Adjacency
Relations` in respect to simplicity and efficiency. Method was promoted
by `Vadim Tropashko`_ in his book `SQL Design Patterns`_. Vadim's
description of the method can be read in his article `Trees in SQL:
Nested Sets and Materialized Path (by Vadim Tropashko)`_.

Implemented features:

    * Setting up with ``declarative.ext`` or without it.
    * Saving node roots -- if no parent set for node.
    * Saving child nodes -- if node has some parent. The whole dirty job
      of setting values in utility fields is done by `sqlamp`.
    * Fetching node's descendants, ancestors and children using the most
      efficient way available.
    * Autochecking exhaustion of tree size limits (maximum number of
      children and maximum nesting level) is done during session flush.
    * Rebuilding all trees and any subtree on the basis of Adjacency
      Relations.
    * Collapsing flat tree returned from query to recursive structure.
    * Node classes may use polymorphic inheritance.
    * Nodes and whole trees/subtrees can be moved around or removed
      entirely.

Known-to-work supported DBMS include `sqlite`_ (tested with 3.6.14),
`MySQL`_ (tested using both MyISAM and InnoDB with server version 5.1.34)
and `PostgreSQL`_ (tested with 8.3.7), but sqlamp should work with any
other DBMS supported by SQLAlchemy.

Supported versions of SQLAlchemy include current minor versions
of branches 0.5 and 0.6 as well as 0.7 since 0.7.2.

.. _`Vadim Tropashko`: http://vadimtropashko.wordpress.com
.. _`Sql Design Patterns`:
   http://www.rampant-books.com/book_2006_1_sql_coding_styles.htm
.. _`Trees in SQL: Nested Sets and Materialized Path (by Vadim Tropashko)`:
   https://communities.bmc.com/communities/docs/DOC-9902
.. _`sqlite`: http://sqlite.org
.. _`MySQL`: http://mysql.com
.. _`PostgreSQL`: http://postgresql.org

"""
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = "0.6"
url = "http://sqlamp.angri.ru"

setup(
    name='sqlamp',
    version=version,
    description="sqlamp is an implementation of " \
                "Materialized Path for SQLAlchemy.",
    long_description=__doc__,
    author='Anton Gritsay',
    author_email='anton@angri.ru',
    url=url,
    download_url="%s/sqlamp-%s.tar.gz" % (url, version),
    packages=['sqlamp'],
    install_requires=['SQLAlchemy >= 0.5'],
    test_suite="tests.run-all.get_suite",

    classifiers=(
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
    ),
    license="BSD",
    platforms="any"
)

