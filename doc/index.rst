.. automodule:: sqlamp

----------
Quickstart
----------

.. note:: Code examples here are all runable by copy-paste
          to interactive interpreter.

.. code-block:: python

    import sqlalchemy, sqlalchemy.orm
    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
    metadata = sqlalchemy.MetaData(engine)

    node_table = sqlalchemy.Table('node', metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('parent_id', sqlalchemy.ForeignKey('node.id')),
        sqlalchemy.Column('name', sqlalchemy.String)
    )

There is nothing special to :mod:`sqlamp` here. Note self-reference
"child to parent" ('parent_id' is foreign key to table's primary key)
just as in any other implementation of adjacency relations.

.. code-block:: python

    import sqlamp
    class Node(object):
        mp = sqlamp.MPManager(node_table)
        def __init__(self, name, parent=None):
            self.name = name
            self.parent = parent
        def __repr__(self):
            return '<Node %r>' % self.name

Attach instance of :class:`~sqlamp.MPManager` to class that represents
node. The only required argument for :class:`~sqlamp.MPManager` constructor
is the table object.

Now we can create the table and define the mapper (it is important
to create table *after* :class:`~sqlamp.MPManager` was created as
created :class:`~sqlamp.MPManager` appends three new columns and one index
to the table):

.. code-block:: python

    node_table.create()

Setting up the mapper requires only one extra step --- providing `Node.mp`
as mapper extension:

.. code-block:: python

    mapper = sqlalchemy.orm.mapper(
        Node, node_table,
        extension=[Node.mp],
        properties={
            'parent': sqlalchemy.orm.relation(
                Node, remote_side=[node_table.c.id]
            )
        }
    )

You may see value provided as `properties` argument: this is a way `recommended
<http://www.sqlalchemy.org/docs/orm/relationships.html#adjacency-list-relationships>`_
by the official SQLAlchemy documentation to set up an adjacency relation.


.. _declarative:

.. rubric:: Alternative way to set up: ext.declarative

Starting from version 0.5 it is able and convenient to use declarative
approach to set your trees up:

.. code-block:: python

    import sqlalchemy, sqlalchemy.orm
    from sqlalchemy.ext.declarative import declarative_base
    import sqlamp

    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
    metadata = sqlalchemy.MetaData(engine)

    BaseNode = declarative_base(metadata=metadata,
                                metaclass=sqlamp.DeclarativeMeta)

    class Node(BaseNode):
        __tablename__ = 'node'
        __mp_manager__ = 'mp'
        id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        parent_id = sqlalchemy.Column(sqlalchemy.ForeignKey('node.id'))
        parent = sqlalchemy.orm.relation("Node", remote_side=[id])
        name = sqlalchemy.Column(sqlalchemy.String())
        def __init__(self, name, parent=None):
            self.name = name
            self.parent = parent
        def __repr__(self):
            return '<Node %r>' % self.name

    Node.__table__.create()

As you can see it is pretty much the same as usual for `sqlalchemy's
"declarative" extension
<http://www.sqlalchemy.org/docs/orm/extensions/declarative.html>`_.
Only two things here are sqlamp-specific: ``metaclass`` argument provided
to ``declarative_base()`` factory function should be :class:`DeclarativeMeta`
and the node class should have an ``__mp_manager__`` property with string
value. See :class:`DeclarativeMeta` for more information about options.

Now all the preparation steps are done. Lets try to use it!

.. code-block:: python

    session = sqlalchemy.orm.sessionmaker(engine)()
    root = Node('root')
    child1 = Node('child1', parent=root)
    child2 = Node('child2', parent=root)
    grandchild = Node('grandchild', parent=child1)
    session.add_all([root, child1, child2, grandchild])
    session.flush()

We have just created a sample tree. This is all about `AL`, nothing
special to :mod:`sqlamp` here. The interesting part is fetching trees:

.. code-block:: python

    root.mp.query_children().all()
    # should print [<Node 'child1'>, <Node 'child2'>]

    root.mp.query_descendants().all()
    # [<Node 'child1'>, <Node 'grandchild'>, <Node 'child2'>]

    grandchild.mp.query_ancestors().all()
    # [<Node 'root'>, <Node 'child1'>]

    Node.mp.query(session).all()
    # [<Node 'root'>, <Node 'child1'>, <Node 'grandchild'>, <Node 'child2'>]

    for node in root.mp.query_descendants(and_self=True):
        print '  ' * node.mp_depth, node.name
    # root
    #   child1
    #     grandchild
    #   child2

As you can see all `sqlamp` functionality is accessible via `MPManager`
descriptor (called `'mp'` in this example).

*Note*: ``Node.mp`` (a so-called "class manager") is not the same
as ``node.mp`` ("instance manager"). Do not confuse them as they are for
different purposes and their APIs has no similar. Class manager (see
:class:`MPClassManager`) exists for features that are not intended
to particular node but for the whole tree: basic setup (mapper
extension) and tree-maintenance functions. And an instance managers
(:class:`MPInstanceManager`) are each unique to and bounded to a node.
They allow to make queries for related nodes and other things
specific to concrete node. There is also third kind of values
that ``MPManager`` descriptor may return, see :class:`its reference
<MPManager>` for more info.


----------------------
Implementation details
----------------------
:mod:`sqlamp` borrowed some implementation ideas from `django-treebeard`_.
In particular, `sqlamp` uses the same alphabet (which consists of numeric
digits and latin-letters in upper case), `sqlamp` as like as `django-treebeard`
doesn't use path parts delimiter --- path parts has fixed adjustable length.
But unlike `django-treebeard` `sqlamp` stores each tree absolutely
stand-alone --- two or more trees may (and will) have identical values in
`path` and `depth` fields and be different only by values in `tree_id` field.
This is the way that can be found in `django-mptt`_.

:mod:`sqlamp` works *only* on basis of Adjacency Relations. This solution
makes data more denormalized but more fault-tolerant. It makes possible
rebuilding all paths for all trees using only `AL` data. Also it makes
applying `sqlamp` on existing project easier.

.. _`django-treebeard`: https://tabo.pe/projects/django-treebeard/
.. _`django-mptt`: http://django-mptt.googlecode.com/


Limits
------
`sqlamp` imposes some limits on the amount of data in the tree.
Those limits are configurable for any specific application.
Here is the list of limits:

*Number of trees:*
    Imposed by :class:`TreeIdField`, which is of type ``INTEGER``.
    Therefore the limit of number of trees is defined by the highest
    integer for DBMS in use. This is not configurable. If you expect
    to have more than 2**31 trees you might want to use custom field
    for tree id with a different numeric type (supposedly ``BIGINT``).
*Number of children in each node:*
    Imposed by the length of one path segment. Can be configured
    using the "steplen" parameter (see :class:`MPManager`). The number
    of children in each node is equal to "36 ** steplen" and with
    default "steplen=3" is equal to 46656. Note that it is not the total
    maximum number of nodes in a tree. Each node can have that much
    children and each of it children can have that much children,
    and so on.
*Maximum nesting depth:*
    Imposed by length of path field and length of one path segment.
    Generally speaking the deepest nesting is equal to maximum possible
    number of path segments in a whole path. So it can be expressed
    as "pathlen // steplen + 1" (see :class:`MPManager`). The default
    value for "pathlen" is for historical reason 255, so together with
    default steplen it sets the maximum depth to 86. Nowadays all major
    DBMS support a higher length for ``VARCHAR`` so you can freely
    increase "pathlen" to, say, 10240 and "pathlen" to 4. These values
    would limit your tree to 1679616 maximum children and 2561 maximum
    depth.
*Total number of nodes in a tree:*
    There is no such limit. Not any that you can hit even with (extremely
    low) default path length. The total number of nodes in a tree
    is equal to "36 ** pathlen" and with "pathlen=255" it is something
    around ``7.2e+397``.


Moving nodes
------------
There are several things to consider on moving nodes with materialized
path. First of all, it can never be as efficient in terms of execution
speed as plain adjacency lists for obvious reason. It can also be slower
than nested sets, because DBMS needs to do more work on rebuilding indices.
Keep in mind that every time you move a subtree, for instance, from one
parent to another, path, tree_id and depth fields have to be updated
in each and every node in the whole tree/subtree you're moving and also
in all subtrees that start from (both old and new) following siblings.
If your application relies on extensive moving of nodes it might be better
to stay with AL.

There is also some points to note in a way the moving of nodes is implemented.
In order to achieve the best performance moving of nodes is not working
on ORM level, instead it uses bulk update queries. The most important
implication of that is that node objects which are stored in session
**do not get updated** after moving is performed. Using them in new queries
will inevitably produce wrong results. Therefore you need to make sure
that you expire all (not only ones that belong to a moved tree!) node
objects from the session after performing any moving or deleting operation.
This implementation detail is reflected in fact that moving operations
are incorporated to :class:`MPClassManager` API (not instance managers)
and accept primary keys instead of node objects.

Another caveat is in limits check. If you put a subtree deep inside another
one it may be possible to overlook that for some nodes the result path
can be longer than accepted value by the path field (see `limits`_ for
details). Unfortunately it is impossible to check in advance without doing
(probably expensive) queries to find out the deepest path in the subtree
that is been moving.

The API for moving nodes comprises the following operations (all of them
are methods of :class:`MPClassManager`):

* :meth:`~MPClassManager.detach_subtree` -- for creating new distinct
  tree from a part of another tree;

* :meth:`~MPClassManager.delete_subtree` -- for deleting subtree
  or whole tree without leaving gaps in paths;

* :meth:`~MPClassManager.move_subtree_before` and
  :meth:`~MPClassManager.move_subtree_after` -- for moving a tree/subtree
  basing on siblings (useful also for changing the child nodes order);

* :meth:`~MPClassManager.move_subtree_to_top` and
  :meth:`~MPClassManager.move_subtree_to_bottom` -- for moving nodes based
  on specified new parent node.

The last four methods raise :exc:`TooManyChildrenError` if new parent node
already has ``36 ** steplen`` children and can not accept one more child
node. They also raise :exc:`MovingToDescendantError` if a new parent node
is one of descendants of moved node.


-------
Support
-------
Feel free to `email author <anton@angri.ru>`_ directly to send bugreports,
feature requests, patches or just to say "thanks"! :)


-------------
API Reference
-------------

.. autoexception:: PathOverflowError
.. autoexception:: TooManyChildrenError
.. autoexception:: PathTooDeepError
.. autoexception:: MovingToDescendantError

.. autoclass:: MPManager(table, parent_id_field=None, path_field='mp_path', depth_field='mp_depth', tree_id_field='mp_tree_id', steplen=3, instance_manager_key='_mp_instance_manager')
    :members: __get__

.. autoclass:: DeclarativeMeta


.. autoclass:: MPClassManager
    :members: max_children, max_depth, query, rebuild_all_trees,
              drop_indices, create_indices,
              detach_subtree, delete_subtree,
              move_subtree_before, move_subtree_after,
              move_subtree_to_top, move_subtree_to_bottom

.. autoclass:: MPInstanceManager
    :members: filter_descendants, query_descendants,
              filter_children, query_children,
              filter_ancestors, query_ancestors

.. autofunction:: tree_recursive_iterator

.. autoclass:: PathField()
.. autoclass:: DepthField()
.. autoclass:: TreeIdField()


---------
Changelog
---------
.. include:: ../CHANGES


.. toctree::
   :maxdepth: 2


