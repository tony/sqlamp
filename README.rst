======
sqlamp
======
An implementation of Materialized Path for SQLAlchemy.


Requirements
------------
Requires SQLAlchemy >= 0.5. Known-to-work supported DBMS include sqlite
(tested with 3.6.14), MySQL (tested using both MyISAM and InnoDB with
server version 5.1.34) and PostgreSQL (tested with 8.3.7), but sqlamp
should work with any other DBMS supported by SQLAlchemy.


Support
-------
Feel free to email author directly to send bugreports, feature requests,
patches, etc.


Installation
------------
To install type as usual::

  python setup.py install

Or drop ``sqlamp`` directory somewhere in your PYTHONPATH.


Documentation
-------------
Documentation for last released version is available online:
`<http://sqlamp.angri.ru>`_. Alternatively you can build and view a full
documentation from project source code by doing `make html` in `doc` dir
and opening the result `_build/html/index.html` file in your browser.


Package Contents
----------------
  doc/
      documentation index in rst-format, can be built using makefile
      included.

  sqlamp/
      source code of the project.

  tests/
      unittests for sqlamp.


License
-------
sqlamp is licensed under 2-clause BSD license, see LICENSE for more
details.

