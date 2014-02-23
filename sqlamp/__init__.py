# coding: utf-8
"""
    `sqlamp` --- Materialized Path for SQLAlchemy
    =============================================

    :author: `Anton Gritsay <anton@angri.ru>`_, http://angri.ru
    :version: %(version)s
    :license: 2-clause BSD (see LICENSE)
    :download: http://sqlamp.angri.ru/sqlamp-%(version)s.tar.gz

    :mod:`sqlamp` is an implementation of an efficient algorithm for working
    with hierarchical data structures --- `Materialized Path`. :mod:`sqlamp`
    uses (and depends of) `SQLAlchemy <http://sqlalchemy.org>`_.

    `Materialized Path` is a way to store (and fetch) a trees in a relational
    databases. It is the compromise between `Nested Sets` and `Adjacency
    Relations` in respect to simplicity and efficiency. Method was promoted
    by `Vadim Tropashko`_ in his book `SQL Design Patterns`_. Vadim's
    description of the method can be read in his article `Trees in SQL:
    Nested Sets and Materialized Path (by Vadim Tropashko)`_.

    Implemented features:

        * Setting up with ``declarative.ext`` or without it.
        * Saving node roots --- if no parent set for node. The tree will have
          a new `tree_id`.
        * Saving child nodes --- if node has some parent. The whole dirty job
          of setting values in `tree_id`, `path` and `depth` fields is done
          by `sqlamp`.
        * Fetching node's descendants, ancestors and children using the most
          efficient way available (see :class:`MPInstanceManager`).
        * Autochecking exhaustion of tree size limits --- maximum number of
          children and maximum nesting level (see :class:`MPManager` to learn
          more about limits fine-tuning) is done during session flush.
        * Rebuilding all trees (see :meth:`MPClassManager.rebuild_all_trees`)
          on the basis of Adjacency Relations.
        * Collapsing flat tree returned from query to recursive structure (see
          :func:`tree_recursive_iterator`).
        * Node classes may use `polymorphic inheritance
          <http://www.sqlalchemy.org/docs/05/mappers.html
          #mapping-class-inheritance-hierarchies>`_.
        * Nodes and whole trees/subtrees can be moved around or removed
          entirely. See `moving nodes`_.

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
import weakref
from operator import attrgetter
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.exc
from sqlalchemy.orm.mapper import class_mapper
from sqlalchemy.ext.declarative import DeclarativeMeta as BaseDeclarativeMeta


__all__ = [
    'MPManager', 'tree_recursive_iterator', 'DeclarativeMeta',
    'PathOverflowError', 'TooManyChildrenError', 'PathTooDeepError'
]

__version__ = (0, 6, 0)
__doc__ %= {'version': '.'.join(map(str, __version__))}

try:
    # Backward compatibility: `all` is new in python 2.5
    all
except NameError:
    def all(iterable):
        for element in iterable:
            if not element:
                return False
        return True
try:
    # The same for `next`.`
    next
except NameError:
    def next(iterable):
        return iterable.next()
try:
    # Forward compatibility: there is no `basestring` in python3
    basestring
except NameError:
    basestring = str


ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
PATH_FIELD_LENGTH = 255
STEP_LENGTH = 3


if hasattr(sqlalchemy.exc, 'DontWrapMixin'):
    # SQLAlchemy 0.7.2+ allows deriving from this special mixin in order to
    # let exceptions raised from types methods during flush pass intact.
    class PathOverflowError(Exception, sqlalchemy.exc.DontWrapMixin):
        "Base class for exceptions in calculations of node's path."
else:
    # SQLAlchemy < 0.7 doesn't need any special base class.
    class PathOverflowError(Exception):
        "Base class for exceptions in calculations of node's path."
    # 0.7 and 0.7.1 wrap exceptions and reraise
    # sqlalchemy.exc.StatementError, so are not fully supported.

class TooManyChildrenError(PathOverflowError):
    "Maximum children limit is exceeded. Raised during flush."

class PathTooDeepError(PathOverflowError):
    "Maximum depth of nesting limit is exceeded. Raised during flush."

class MovingToDescendantError(RuntimeError):
    """
    An attempt to move a tree inside of one of its descendants was made.

    See `moving nodes`_.
    """


def inc_path(path, steplen):
    """
    Simple arithmetical operation --- incrementation of an integer number
    (with radix of `len(ALPHABET)`) represented as string.

    :param path:
        `str`, the path to increment.
    :param steplen:
        `int`, the number of maximum characters to carry overflow.
    :returns:
        new path which is greater than `path` by one.
    :raises PathOverflowError:
        when incrementation of `path` cause to carry overflow by number
        of characters greater than `steplen`.

    >>> inc_path('0000', 4)
    '0001'
    >>> inc_path('3GZU', 4)
    '3GZV'
    >>> inc_path('337Z', 2)
    '3380'
    >>> inc_path('GWZZZ', 5)
    'GX000'
    >>> import unittest
    >>> unittest.TestCase('id').assertRaises(PathOverflowError, \
                                             inc_path, 'ABZZ', 2)
    """
    parent_path, path = path[:-steplen], path[-steplen:]
    path = path.rstrip(ALPHABET[-1])
    if not path:
        raise PathOverflowError()
    zeros = steplen - len(path)
    path = path[:-1] + \
           ALPHABET[ALPHABET.index(path[-1]) + 1] + \
           ALPHABET[0] * zeros
    return parent_path + path


class MPOptions(object):
    """
    A container for options for one tree table.

    :param _attach_columns:
        Bool, if ``True`` then created columns will be automatically
        attached to the table and indices will be declared right away
        in the object's constructor. Otherwise columns should
        be attached later manually and indices should be created
        by calling :meth:`declare_indices`.

    For other parameters description see :class:`MPManager`.
    """
    def __init__(self,
                 table,
                 parent_id_field=None,
                 path_field='mp_path',
                 depth_field='mp_depth',
                 tree_id_field='mp_tree_id',
                 steplen=None,
                 pathlen=None,
                 _attach_columns=True):

        self.table = table

        if steplen is not None:
            self.steplen = steplen
        else:
            self.steplen = STEP_LENGTH
        # pathlen is set later, after creating a column object

        assert len(table.primary_key.columns) == 1, \
               "Composite primary keys are not supported"
        [self.pk_field] = table.primary_key.columns

        if parent_id_field is None:
            self.parent_id_field = table.join(table).onclause.right
        elif isinstance(parent_id_field, basestring):
            self.parent_id_field = table.columns[parent_id_field]
        else:
            assert isinstance(parent_id_field, sqlalchemy.Column)
            assert parent_id_field.table is table
            self.parent_id_field = parent_id_field

        # If path length was not provided, we omit passing it to checker
        # function and let :class:`PathField` set the default length.
        path_params = {}
        if pathlen is not None:
            path_params = {'length': pathlen}

        self.path_field = self.check_or_create_field(
            table, 'path', path_field, PathField, _attach_columns, path_params
        )
        self.depth_field = self.check_or_create_field(
            table, 'depth', depth_field, DepthField, _attach_columns
        )
        self.tree_id_field = self.check_or_create_field(
            table, 'tree_id', tree_id_field, TreeIdField, _attach_columns
        )
        self.fields = (self.path_field, self.depth_field, self.tree_id_field)

        # Getting path length from the actual column length, no matter if
        # we're dealing with custom path field object, or just created one.
        self.pathlen = self.path_field.type.length
        self.max_children = len(ALPHABET) ** self.steplen
        self.max_depth = (self.pathlen // self.steplen) + 1

        if _attach_columns:
            self.declare_indices()

    def declare_indices(self):
        """
        Populate object's "indices" property with sqlamp specific indices
        (as of 0.6 there is only one). Appends created indices to the table.
        """
        self.indices = [
            sqlalchemy.Index(
                '__'.join((self.table.name, self.tree_id_field.name,
                           self.path_field.name)),
                self.tree_id_field,
                self.path_field,
                unique=True
            ),
        ]
        for index in self.indices:
            self.table.append_constraint(index)

    @classmethod
    def check_or_create_field(cls, table, name, field, type_,
                              attach, params=None):
        """
        Check field argument (one of `path_field`, `depth_field` and
        `tree_id_field`), convert it from field name to `Column` object
        if needed, create the column object if needed and check the
        existing `Column` object for sanity.
        """
        assert field is not None
        if not isinstance(field, basestring):
            assert isinstance(field, sqlalchemy.Column)
            assert not attach or field.table is table
        elif field in table.columns:
            field = table.columns[field]
            if params:
                # User wants to use custom-made column object,
                # but at the same time provides a list of parameters
                # for column's type engine as if they want us
                # to create it. It doesn't make any sense, because
                # type is already created and most likely the column
                # with parameters provided there already exists
                # in the database. Anyway, if values are consistent,
                # we won't complain.
                actual_params = dict((k, getattr(field.type, k))
                                     for k in params)
                assert actual_params == params, \
                       "Can not change parameters from %r to %r " \
                       "on field %s" % (actual_params, params, name)
        else:
            field = sqlalchemy.Column(field, type_(**(params or {})),
                                      nullable=False)
            if attach:
                table.append_column(field)
            return field
        assert isinstance(field.type, type_), \
               "The type of %s field should be %r" % (name, type_)
        assert not field.nullable, \
               "The %s field should not be nullable" % name
        return field

    def query(self, entities, session):
        """
        Create and return `sqlalchemy.org.Query` object, passing arguments
        to its constructor.

        The query object is returned being ordered by `tree_id_field`
        and `path_field`.
        """
        return sqlalchemy.orm.Query(entities, session) \
                             .order_by(None) \
                             .order_by(self.tree_id_field, self.path_field)

    def filter_descendants(self, tree_id, path, and_self):
        """
        Get a filter condition for descendants of node with known
        `path` and `tree_id`.

        :param and_self:
            `bool`, if set to `True` node with path `path` will be also
            selected by filter.
        :return:
            a filter clause.
        """
        # we are not using queries like `WHERE path LIKE '0.1.%` instead
        # they looks like `WHERE path > '0.1' AND path < '0.2'`
        try:
            next_sibling_path = inc_path(path, self.steplen)
        except PathOverflowError:
            # this node is theoretically last, will not check
            # for `path < next_sibling_path`
            next_sibling_path = None
        # always filter by `tree_id`:
        filter_ = self.tree_id_field == tree_id
        if and_self:
            # non-strict inequality if this node should satisfy filter
            filter_ &= self.path_field >= path
        else:
            filter_ &= self.path_field > path
        if next_sibling_path is not None:
            filter_ &= self.path_field < next_sibling_path
        return filter_

    def filter_children(self, tree_id, path, depth):
        """
        The same as :meth:`filter_descendants` but filters children nodes
        and does not accepts `and_self`, but accepts `depth`.
        """
        # Oh yeah, using adjacency relation may be more efficient here. But
        # one can access AL-based children collection without `sqlamp` at all.
        # And in that case we can be sure that at least `(tree_id, path)`
        # index is used. `parent_id` field may not have index set up so
        # condition `pk == parent_id` in SQL query may be even less efficient.
        filter_ = self.filter_descendants(tree_id, path, and_self=False)
        filter_ &= self.depth_field == (depth + 1)
        return filter_

    def filter_ancestors(self, tree_id, path, depth, and_self):
        "The same as :meth:`filter_descendants` but filters ancestor nodes."
        # WHERE tree_id = <node.tree_id> AND <node.path> LIKE path || '%'
        filter_ = (self.tree_id_field == tree_id) \
                  & sqlalchemy.sql.expression.literal(
                        path, sqlalchemy.String
                    ).like(self.path_field + '%')
        if and_self:
            filter_ &= self.depth_field  <= depth
        else:
            filter_ &= self.depth_field < depth
        return filter_

    def filter_parent(self, parent_id):
        "Get a filter condition for a node's parent."
        if parent_id is None:
            return sqlalchemy.sql.literal(False)
        filter_ = self.pk_field == parent_id
        return filter_

class _InsertionsParamsSelector(object):
    """
    Instances of this class used as values for :class:`TreeIdField`,
    :class:`PathField` and :class:`DepthField` when tree nodes are
    created. It makes a "lazy" query to determine actual values for
    this fields.

    :param opts: instance of :class:`MPOptions`.
    :param session: session which will be used for query.
    :param parent_id: parent's node primary key, may be `None`.
    """
    def __init__(self, opts, session, parent_id):
        self._mp_opts = opts
        self.parent_id = parent_id
        self.session = session
        self._query_result = None

    def _perform_query(self):
        """
        Make a query, get an actual values for `path`, `tree_id`
        and `depth` fields and put them in dict `self._query_result`.
        """
        opts = self._mp_opts
        if self.parent_id is None:
            # a new instance will be a root node.
            # `tree_id` is next unused integer value,
            # `depth` for root nodes is equal to zero,
            # `path` should be empty string.
            [tree_id] = self.session.execute(
                sqlalchemy.func.max(opts.tree_id_field)
            ).fetchone()
            if tree_id is None:
                tree_id = 1
            else:
                tree_id += 1
            self._query_result = dict(path='', depth=0, tree_id=tree_id)
        else:
            # a new instance has at least one ancestor.
            # `tree_id` can be used from parent's value,
            # `depth` is parent's depth plus one,
            # `path` will be calculated from two values -
            # the path of the parent node itself and it's
            # last child's path.
            query = self.session.execute(sqlalchemy.select(
                [
                    opts.tree_id_field.label('tree_id'),
                    (opts.depth_field + 1).label('depth'),
                    opts.path_field.label('parent_path'),
                    sqlalchemy.select(
                        [sqlalchemy.func.max(opts.path_field)],
                        opts.parent_id_field == self.parent_id
                    ).label('last_child_path'),
                ],
                opts.pk_field == self.parent_id
            )).fetchone()
            steplen = self._mp_opts.steplen
            if not query['last_child_path']:
                # node is the first child.
                path = query['parent_path'] + ALPHABET[0] * steplen
            else:
                try:
                    path = inc_path(query['last_child_path'], steplen)
                except PathOverflowError:
                    # transform exception `PathOverflowError`, raised by
                    # `inc_path()` to more convenient `TooManyChildrenError`.
                    raise TooManyChildrenError()
            if len(path) > opts.pathlen:
                raise PathTooDeepError()
            self._query_result = dict(
                path=path, depth=query['depth'], tree_id=query['tree_id']
            )

    @property
    def query_result(self):
        """
        Get query result dict, calling `self._perform_query()`
        for the first time.
        """
        if self._query_result is None:
            self._perform_query()
        return self._query_result


class TreeIdField(sqlalchemy.types.TypeDecorator):
    "Integer field subtype representing node's tree identifier."
    impl = sqlalchemy.Integer
    def process_bind_param(self, value, dialect):
        if not isinstance(value, _InsertionsParamsSelector):
            return value
        return value.query_result['tree_id']

class DepthField(sqlalchemy.types.TypeDecorator):
    "Integer field subtype representing node's depth level."
    impl = sqlalchemy.Integer
    def process_bind_param(self, value, dialect):
        if not isinstance(value, _InsertionsParamsSelector):
            return value
        return value.query_result['depth']

class PathField(sqlalchemy.types.TypeDecorator):
    "Varchar field subtype representing node's path."
    impl = sqlalchemy.String
    def __init__(self, length=None):
        if length is None:
            length = PATH_FIELD_LENGTH
        super(PathField, self).__init__(length)
    def process_bind_param(self, value, dialect):
        if not isinstance(value, _InsertionsParamsSelector):
            return value
        return value.query_result['path']
    def adapt_operator(self, op):
        # required for concatenation to work right
        return self.impl.adapt_operator(op)


class MPMapperExtension(sqlalchemy.orm.interfaces.MapperExtension):
    """
    An extension to node class' mapper.

    :param opts: instance of :class:`MPOptions`
    """
    def __init__(self, opts):
        super(MPMapperExtension, self).__init__()
        self._mp_opts = opts

    def before_insert(self, mapper, connection, instance):
        """
        Creates an :class:`_InsertionsParamsSelector` instance and
        sets values of tree_id, depth and path fields to it.
        """
        opts = self._mp_opts
        parent = getattr(instance, opts.parent_id_field.name)
        tree_id = depth = path = _InsertionsParamsSelector(
            opts, sqlalchemy.orm.session.object_session(instance), parent
        )
        setattr(instance, opts.tree_id_field.name, tree_id)
        setattr(instance, opts.path_field.name, path)
        setattr(instance, opts.depth_field.name, depth)

    def after_insert(self, mapper, connection, instance):
        """
        Replaces :class:`_InsertionsParamsSelector` instance (which
        is remains after flush) with actual values of tree_id, depth
        and path fields.
        """
        opts = self._mp_opts
        params_selector = getattr(instance, opts.path_field.name)
        assert isinstance(params_selector, _InsertionsParamsSelector)
        query_result = params_selector.query_result
        setattr(instance, opts.tree_id_field.name, query_result['tree_id'])
        setattr(instance, opts.path_field.name, query_result['path'])
        setattr(instance, opts.depth_field.name, query_result['depth'])


class MPClassManager(object):
    """
    Node class manager. No need to create it by hand: it's created
    by :class:`MPManager`.

    :param node_class: class which was mapped to the tree table.
    :param opts: instance of :class:`MPOptions`.

    .. versionchanged::
        0.6
        Previously existing method `__clause_element__` which used to allow
        using the instances of :class:`MPClassManager` as arguments to methods
        `query.order_by()` was removed in 0.6. Use :meth:`query` instead.
    """
    def __init__(self, node_class, opts):
        self._mp_opts = opts
        self.node_class = node_class

    @property
    def max_children(self):
        "The maximum number of children in each node, readonly."
        return self._mp_opts.max_children
    @property
    def max_depth(self):
        "The maximum level of nesting in this tree, readonly."
        return self._mp_opts.max_depth

    def detach_subtree(self, session, node_id):
        """
        Create a new distinct tree with root ``node_id``.

        Expects that new root node is a part of another tree (has non-empty
        parent id). New root node and all its descendants will have new
        `tree_id` and adjusted paths.

        :param session:
            session object for DML queries.
        :param node_id:
            primary key of to-be-new-root node.

        See also general notes on `moving nodes`_.
        """
        opts = self._mp_opts
        [[old_parent_id, old_path, old_depth, old_tree_id]] = session.execute(
            sqlalchemy.select([opts.parent_id_field, opts.path_field,
                               opts.depth_field, opts.tree_id_field],
                               opts.pk_field == node_id)
        )
        assert old_parent_id, "Node %s is already a root of own tree" % node_id

        # new tree will have next available tree_id
        new_tree_id = session.execute(sqlalchemy.select([
            sqlalchemy.func.max(opts.tree_id_field) + 1
        ])).scalar()
        self._reparent(session, node_id, new_parent_id=None,
                       new_tree_id=new_tree_id, new_path='', new_depth=0,
                       old_tree_id=old_tree_id, old_path=old_path,
                       old_depth=old_depth)

    def delete_subtree(self, session, node_id):
        """
        Delete a whole tree/subtree starting from root ``node_id``.

        :param session:
            session object for DML queries.
        :param node_id:
            primary key of root of tree/subtree to be deleted.

        This method differs from performing something like ::

            # leaves gaps!
            node.mp.query_descendants(session, and_self=True).delete()

        because it updates the paths of following siblings in order
        to make sure that the tree limits are not lowered artificially
        no matter how many deletion operations have been performed.
        Therefore the shown code is not recommended way for deleting
        nodes. Use :meth:`delete_subtree` instead.

        .. note::
            If you use MySQL InnoDB you need to set child-to-parent foreign
            key as ``ON DELETE CASCADE`` in order for this method to work
            for non-empty trees/subtrees. This is because of InnoDB's
            inability to defer constraint check to the end of statement
            execution. See `relevant innodb docs`__.

        See also general notes on `moving nodes`_.

        .. __:
            http://dev.mysql.com/doc/refman/5.5/en/innodb-foreign-key-constraints.html
        """
        opts = self._mp_opts
        [[old_path, old_depth, old_tree_id]] = session.execute(
            sqlalchemy.select([opts.path_field, opts.depth_field,
                               opts.tree_id_field], opts.pk_field == node_id)
        )
        filter_ = opts.filter_descendants(old_tree_id, old_path, and_self=True)
        session.execute(sqlalchemy.delete(opts.table, filter_))
        self._pull_nodes('up', session, old_tree_id, old_path, old_depth)

    def move_subtree_before(self, session, node_id, anchor_id):
        """
        Move tree/subtree starting from ``node_id`` to make it preceding
        sibling of node with pk ``anchor_id``. Anchor node is expected not
        to be a root of own tree.

        :param session:
            session object for DML queries.
        :param node_id:
            primary key of root of tree/subtree to be moved.
        :param anchor_id:
            primary key of a node which target node should become previous
            sibling to.

        See also general notes on `moving nodes`_.
        """
        self._move_subtree_by_sibling('before', session, node_id, anchor_id)

    def move_subtree_after(self, session, node_id, anchor_id):
        """
        The same as :meth:`move_subtree_before` but makes target tree/subtree
        root the immediately following sibling of anchor node.
        """
        self._move_subtree_by_sibling('after', session, node_id, anchor_id)

    def _move_subtree_by_sibling(self, before_or_after, session,
                                 node_id, anchor_id):
        """
        The common code for :meth:`move_subtree_before`
        and :meth:`move_subtree_after`.

        :param before_or_after: string, either 'before' or 'after'.
        """
        opts = self._mp_opts
        old_path, old_depth, old_tree_id, \
            new_parent_id, anchors_path, new_depth, new_tree_id \
            = self._prepare_to_move_subtree(session, node_id, anchor_id)

        # We do not impose ordering of distinct trees, so even though
        # technically roots of different trees are siblings, changing
        # they order with "move_subtree_(before|after)" is not supported.
        assert new_parent_id is not None, \
               "Use detach_subtree() for creating a new distinct tree"

        if before_or_after == 'after':
            anchors_path = inc_path(anchors_path, opts.steplen)
        else:
            assert before_or_after == 'before'

        # Freeing a place for target node.
        self._pull_nodes('down', session, new_tree_id, anchors_path, new_depth)
        # Target node could be thw following sibling or to be the descendant
        # of one of following siblings. In that case its path has been updated
        # on the previous step, so we need to fetch it again. If the target
        # node belongs to different tree or to one of previous siblings,
        # this query is redundant, but harmless.
        [[old_path]] = session.execute(
            sqlalchemy.select([opts.path_field], opts.pk_field == node_id)
        )
        new_path = anchors_path
        self._reparent(session, node_id, new_parent_id, new_tree_id, new_path,
                       new_depth, old_tree_id, old_path, old_depth)

    def move_subtree_to_top(self, session, node_id, new_parent_id):
        """
        Move tree/subtree starting from ``node_id`` to make it the first child
        of node with pk ``anchor_id``.

        :param session:
            session object for DML queries.
        :param node_id:
            primary key of root of tree/subtree to be moved.
        :param anchor_id:
            primary key of a node which should become a new parent
            for target node.

        See also general notes on `moving nodes`_.
        """
        opts = self._mp_opts
        old_path, old_depth, old_tree_id, \
            parents_parent_id, parents_path, parents_depth, new_tree_id \
            = self._prepare_to_move_subtree(session, node_id, new_parent_id)
        new_depth = parents_depth + 1

        new_path = parents_path + ALPHABET[0] * opts.steplen
        # Pulling down all new parent's children.
        self._pull_nodes('down', session, new_tree_id, new_path, new_depth)
        # Updating target node's path (see _move_subtree_by_sibling).
        [[old_path]] = session.execute(
            sqlalchemy.select([opts.path_field], opts.pk_field == node_id)
        )
        self._reparent(session, node_id, new_parent_id, new_tree_id, new_path,
                       new_depth, old_tree_id, old_path, old_depth)

    def move_subtree_to_bottom(self, session, node_id, new_parent_id):
        """
        The same as :meth:`move_subtree_before` but makes target tree/subtree
        root the last child of anchor node.
        """
        opts = self._mp_opts
        old_path, old_depth, old_tree_id, \
            parents_parent_id, parents_path, parents_depth, new_tree_id \
            = self._prepare_to_move_subtree(session, node_id, new_parent_id)
        new_depth = parents_depth + 1

        children_filter = opts.filter_children(new_tree_id, parents_path,
                                               parents_depth)
        last_child_path = session.execute(
            sqlalchemy.select([opts.path_field], children_filter) \
                      .order_by(opts.tree_id_field.desc(),
                                opts.path_field.desc()) \
                      .limit(1)
        ).fetchall()
        if not last_child_path:
            # The new parent doesn't have any child nodes.
            # Target node will be the first.
            new_path = parents_path + ALPHABET[0] * opts.steplen
        else:
            # Target node path will be the next after last child.
            [[last_child_path]] = last_child_path
            try:
                new_path = inc_path(last_child_path, opts.steplen)
            except PathOverflowError:
                raise TooManyChildrenError()
        self._reparent(session, node_id, new_parent_id, new_tree_id, new_path,
                       new_depth, old_tree_id, old_path, old_depth)

    def _prepare_to_move_subtree(self, session, node_id, anchor_id):
        """
        Fetch target and anchor nodes data and check that moving operation
        is valid for that pair of nodes.

        :raises MovingToDescendantError:
            If anchor node is descendant of target node.
        :returns:
            7-element tuple with the path, depth, tree_id of target node
            and parent_id, path, depth, tree_id of anchor node.
        """
        opts = self._mp_opts
        columns = [opts.parent_id_field, opts.path_field,
                   opts.depth_field, opts.tree_id_field]
        node_select = sqlalchemy.select(columns, opts.pk_field == node_id)
        anchor_select = sqlalchemy.select(columns, opts.pk_field == anchor_id)
        [[old_parent_id, old_path, old_depth, old_tree_id],
         [anchor_parent_id, anchor_path, anchor_depth, anchor_tree_id]] \
                 = session.execute(node_select.union_all(anchor_select))
        if old_tree_id == anchor_tree_id and anchor_path.startswith(old_path):
            raise MovingToDescendantError()
        return old_path, old_depth, old_tree_id, \
               anchor_parent_id, anchor_path, anchor_depth, anchor_tree_id

    def _reparent(self, session, node_id, new_parent_id, new_tree_id, new_path,
                  new_depth, old_tree_id, old_path, old_depth):
        """
        Update node's parent_id , then :meth:`_update_subtree` and then fill
        the gap left from moving a subtree to a new place by pulling following
        siblings up.
        """
        opts = self._mp_opts
        session.execute(
            opts.table.update().where(opts.pk_field == node_id) \
                               .values({opts.parent_id_field: new_parent_id})
        )
        self._update_subtree(session, node_id, new_tree_id, new_path,
                             new_depth, old_tree_id, old_path, old_depth)
        self._pull_nodes('up', session, old_tree_id, old_path, old_depth)

    def _update_subtree(self, session, node_id, new_tree_id, new_path,
                        new_depth, old_tree_id, old_path, old_depth):
        """
        Update subtree (starting from node ``node_id``) nodes' depth, path
        and tree_id.

        The method doesn't deal with recalculating paths, it only can cut
        and/or concatenate them.
        """
        opts = self._mp_opts
        descendants_filter = opts.filter_descendants(old_tree_id, old_path,
                                                     and_self=True)
        depth_delta = new_depth - old_depth
        # Will use updates with sql expressions
        new_depth = opts.depth_field + depth_delta
        new_path_expr = sqlalchemy.func.substr(
            opts.path_field, opts.steplen * old_depth + 1
        )
        # this is needed for concatenation of function
        # and literal to work with SQLAlchemy 0.5.x
        new_path_expr.type = sqlalchemy.String()
        new_path = new_path + new_path_expr
        session.execute(
            opts.table.update().where(descendants_filter) \
                               .values({opts.tree_id_field: new_tree_id,
                                        opts.depth_field: new_depth,
                                        opts.path_field: new_path})
        )

    def _pull_nodes(self, up_or_down, session, tree_id, from_path, depth):
        """
        Move all nodes in tree ``tree_id`` starting from path ``from_path``
        with depth ``depth`` and same parent as node with path ``from_path``
        either one step up or one step down.

        This is used to free a space for node which is been moving to land
        (moving down) as well as to remove a gap from that node's old place
        (moving up).
        """
        if depth == 0:
            # roots have no siblings, nothing to pull in either direction
            return

        opts = self._mp_opts

        # TODO: move this to MPOptions.filter_siblings_after()
        filter_ = (opts.tree_id_field == tree_id) & \
                  (opts.path_field >= from_path) & \
                  (opts.depth_field == depth)
        parent_path = from_path[:-opts.steplen]
        if parent_path:
            filter_ &= (opts.path_field < inc_path(parent_path, opts.steplen))

        nodes = session.execute(
            sqlalchemy.select([opts.pk_field, opts.path_field], filter_) \
                      .order_by(opts.tree_id_field, opts.path_field)
        )
        # We can't do path math at sql side without resorting to DBMS-specific
        # things or to using stored procedures. Therefore we have to process
        # siblings sequentially.
        if up_or_down == 'down':
            # Moving several nodes in a row down has to be done
            # in backwards direction.
            nodes = list(reversed(nodes.fetchall()))
            if not nodes:
                return
            _, lastnodepath = nodes[0]
            try:
                prev_path = inc_path(lastnodepath, opts.steplen)
            except PathOverflowError:
                # The last sibling is the last possible node.
                raise TooManyChildrenError()
        else:
            assert up_or_down == 'up'
            prev_path = from_path
        for [node_id, path] in nodes:
            self._update_subtree(session, node_id, tree_id, prev_path,
                                 depth, tree_id, path, depth)
            prev_path = path

    def _do_rebuild_subtree(self, session, root_node_id, root_path,
                            root_depth, tree_id, order_by):
        """
        The main recursive function for rebuilding trees.

        :param session:
            session object for DML queries.
        :param root_node_id:
            subtree's root node primary key value.
        :param root_path:
            the pre-calculated path of root node.
        :param root_depth:
            the pre-calculated root node's depth.
        :param tree_id:
            the pre-calculated identifier for this tree.
        :param order_by:
            the children sort order.
        """
        opts = self._mp_opts
        path = root_path + ALPHABET[0] * opts.steplen
        depth = root_depth + 1
        children = session.execute(sqlalchemy.select(
            [opts.pk_field],
            opts.parent_id_field == root_node_id
        ).order_by(order_by))
        query = opts.table.update()
        for child in children.fetchall():
            [child] = child
            session.execute(
                query.where(opts.pk_field == child) \
                     .values({opts.path_field: path, \
                              opts.depth_field: depth, \
                              opts.tree_id_field: tree_id})
            )
            self._do_rebuild_subtree(session, child, path, depth,
                                     tree_id, order_by)
            path = inc_path(path, opts.steplen)

    def drop_indices(self, session):
        """
        Drop mp-related indices.

        Note that in general you need this only in conjunction
        with :meth:`rebuild_all_trees`, which this method used
        to be a part of.

        :param session:
            sqlalchemy `Session` object to bind DDL queries.

        .. versionadded:: 0.6
        """
        for index in self._mp_opts.indices:
            index.drop(bind=session.bind)

    def create_indices(self, session):
        """
        Create mp-related indices.

        Note that needed indices are created by default if you
        use sqlalchemy's DDL facility (like ``table.create()``)
        on mp-armed table. This method is useful after call
        to :meth:`rebuild_all_trees`, or when you're setting up
        sqlamp on an existing table.

        :param session:
            sqlalchemy `Session` object to bind DDL queries.

        .. versionadded:: 0.6
        """
        for index in self._mp_opts.indices:
            index.create(bind=session.bind)

    def rebuild_all_trees(self, session, order_by=None):
        """
        Perform a complete rebuild of all trees on the basis
        of adjacency relations.

        :param session:
            a session object which will be used for DML-queries. The session's
            transaction gets commited when rebuilding is done.
        :param order_by:
            an "order by clause" for sorting root nodes and children nodes
            in each subtree. By default ordering by primary key is used.

        .. versionchanged::
            0.6
            :meth:`rebuild_all_trees` didn't receive ``session`` parameter
            prior to 0.6.

        .. warning::
            This method no longer drops/creates indices!

        .. versionchanged::
            0.6
            Before 0.6 this method was dropping mp-related indices before
            starting to modify table content and recreating them afterwards.
            Now these parts are factored out to :meth:`drop_indices`
            and :meth:`create_indices` respectively.
        """
        opts = self._mp_opts
        order_by = order_by or opts.pk_field
        roots = session.execute(sqlalchemy.select(
            [opts.pk_field], opts.parent_id_field == None
        ).order_by(order_by))
        update_query = opts.table.update()
        for tree_id, root_node in enumerate(roots.fetchall()):
            [node_id] = root_node
            # resetting path, depth and tree_id for root node:
            session.execute(
                update_query.where(opts.pk_field == node_id) \
                            .values({opts.tree_id_field: tree_id + 1,
                                     opts.path_field: '',
                                     opts.depth_field: 0}) \
            )
            self._do_rebuild_subtree(session, node_id, '', 0,
                                     tree_id + 1, order_by)
        session.commit()

    def query(self, session):
        """
        Query all stored trees.

        :param session: a sqlalchemy `Session` object to bind a query.
        :returns:
            `Query` object with all nodes of all trees sorted as usual
            by `(tree_id, path)`.

        .. versionchanged::
            0.6
            Before 0.6 this method was called ``query_all_trees``. The old
            name still works for backward compatibility.
        """
        return self._mp_opts.query(self.node_class, session)
    query_all_trees = query


def _get_none():
    # used as a result callable for MPInstanceManager.__reduce__
    return None
_get_none.__safe_for_unpickling__ = True


class MPInstanceManager(object):
    """
    A node instance manager, unique for each node. First created
    on access to :class:`MPManager` descriptor from instance.
    Implements API to query nodes related somehow to particular
    node: descendants, ancestors, etc.

    :param opts:
        instance of :class:`MPOptions`.
    :param root_node_class:
        the root class in the node class' polymorphic inheritance hierarchy.
        This class will be used to perform queries.
    :param obj:
        particular node instance.
    """
    __slots__ = ('_mp_opts', '_obj_ref', '_root_node_class')

    def __reduce__(self):
        # Return a function which returns ``None``. This effectively makes
        # unpickling of MPInstanceManager objects have result of ``None``.
        return (_get_none, ())

    def __init__(self, opts, root_node_class, obj):
        self._root_node_class = root_node_class
        self._mp_opts = opts
        self._obj_ref = weakref.ref(obj)

    def _get_obj(self):
        "Dereference weakref and return node instance."
        return self._obj_ref()

    def _get_query(self, obj, session):
        """
        Get a query for the node's class.

        If :attr:`session` is `None` tries to use :attr:`obj`'s session,
        if it is available.

        :param session: a sqlalchemy `Session` object or `None`.
        :return: an object `sqlalchemy.orm.Query`.
        :raises AssertionError:
            if :attr:`session` is `None` and node is not bound
            to a session.
        """
        obj_session = self._get_session_and_assert_flushed(obj)
        if session is None:
            # use node's session only if particular session
            # was not specified
            session = obj_session
        return self._mp_opts.query(self._root_node_class, session=session)

    def _get_session_and_assert_flushed(self, obj):
        """
        Ensure that node has "real" values in its `path`, `tree_id`
        and `depth` fields and return node's session.

        Determines object session, flushs it if instance is in "pending"
        state and session has `autoflush == True`. Flushing is needed
        for instance's `path`, `tree_id` and `depth` fields hold real
        values applicable for queries. If the node is not bound to a
        session tries to check that it was "persistent" once upon a time.

        :return: session object or `None` if node is in "detached" state.
        :raises AssertionError:
            if instance is in "pending" state and session has `autoflush`
            disabled.
        :raises AssertionError:
            if instance is in "transient" state (has no "persistent" copy
            and is not bound to a session).
        """
        session = sqlalchemy.orm.session.object_session(obj)
        if session is not None:
            if obj in session.new:
                assert session.autoflush, \
                        "instance %r is in 'pending' state and attached " \
                        "to non-autoflush session. call `session.flush()` " \
                        "to be able to get filters and perform queries." % obj
                session.flush()
        else:
            assert all(getattr(obj, field.name) is not None \
                       for field in self._mp_opts.fields), \
                    "instance %r seems to be in 'transient' state. " \
                    "put it in the session to be able to get filters " \
                    "and perform queries." % obj
        return session

    def _get_values(self):
        """
        Perform :meth:`_get_session_and_assert_flushed`, get values
        of `tree_id`, `path` and `depth` fields and return them as a tuple.

        Used in `filter_*` methods.
        """
        opts = self._mp_opts
        obj = self._get_obj()
        self._get_session_and_assert_flushed(obj)
        tree_id = getattr(obj, opts.tree_id_field.name)
        path = getattr(obj, opts.path_field.name)
        depth = getattr(obj, opts.depth_field.name)
        return tree_id, path, depth

    def filter_descendants(self, and_self=False):
        """
        Get a filter condition for node's descendants.

        Requires that node has `path`, `tree_id` and `depth` values
        available (that means it has "persistent version" even if the
        node itself is in "detached" state or it is in "pending" state
        in `autoflush`-enabled session).

        Usage example::

            Node.mp.query(session).filter(root.mp.filter_descendants())

        This example is silly and only shows an approach of using
        `filter_descendants`. Don't use it for such purpose as there is
        a better way for such simple queries: :meth:`query_descendants`.

        :param and_self:
            `bool`, if set to `True` self node will be selected by filter.
        :return:
            a filter clause applicable as argument for
            `sqlalchemy.orm.Query.filter()` and others.
        """
        tree_id, path, depth = self._get_values()
        return self._mp_opts.filter_descendants(tree_id, path, and_self)

    def query_descendants(self, session=None, and_self=False):
        """
        Get a query for node's descendants.

        Requires that node is in "persistent" state or in "pending"
        state in `autoflush`-enabled session.

        :param session:
            session object for query. If not provided, node's session is
            used. If node is in "detached" state and :attr:`session` is
            not provided, query will be detached too (will require setting
            `session` attribute to execute).
        :param and_self:
            `bool`, if set to `True` self node will be selected by query.
        :return:
            a `sqlalchemy.orm.Query` object which contains only node's
            descendants and is ordered by `path`.
        """
        return self._get_query(self._get_obj(), session) \
                   .filter(self.filter_descendants(and_self=and_self))

    def filter_children(self):
        """
        The same as :meth:`filter_descendants` but filters children nodes
        and does not accepts :attr:`and_self` parameter.
        """
        tree_id, path, depth = self._get_values()
        return self._mp_opts.filter_children(tree_id, path, depth)

    def query_children(self, session=None):
        """
        The same as :meth:`query_descendants` but queries children nodes and
        does not accepts :attr:`and_self` parameter.
        """
        return self._get_query(self._get_obj(), session) \
                   .filter(self.filter_children())

    def filter_ancestors(self, and_self=False):
        "The same as :meth:`filter_descendants` but filters ancestor nodes."
        tree_id, path, depth = self._get_values()
        return self._mp_opts.filter_ancestors(tree_id, path, depth, and_self)

    def query_ancestors(self, session=None, and_self=False):
        "The same as :meth:`query_descendants` but queries node's ancestors."
        return self._get_query(self._get_obj(), session) \
                   .filter(self.filter_ancestors(and_self=and_self)) \
                   .order_by(None).order_by(self._mp_opts.depth_field)

    def filter_parent(self):
        "Get a filter condition for a node's parent."
        opts = self._mp_opts
        obj = self._get_obj()
        self._get_session_and_assert_flushed(obj)
        parent_id = getattr(obj, opts.parent_id_field.name)
        return self._mp_opts.filter_parent(parent_id)


class MPManager(object):
    """
    Descriptor for access class-level and instance-level API.

    Basic usage is simple::

        class Node(object):
            mp = sqlamp.MPManager(node_table)

    Now there is an ability to get instance manager or class manager
    via property `'mp'` depending on way to access it. `Node.mp` will
    return mapper extension till class is mapped, class manager
    :class:`MPClassManager` after that and `instance_node.mp`
    will return instance_node's :class:`MPInstanceManager`.
    See that classes for more details about their public API.

    .. versionchanged:: 0.5.1
        Previously mapper extension was accessible via class manager's
        property.

    :param table:
        instance of `sqlalchemy.Table`. A table that will be mapped to
        node class and will hold tree nodes in its rows. It is the only
        one strictly required argument.

    :param parent_id_field=None:
        a foreign key field that is reference to parent node's
        primary key. If this parameter is omitted, it will be guessed
        joining a `table` with itself and using the right part of join's
        onclause as parent id field.

    :param path_field='mp_path':
        the name for the path field or the field object itself. The field
        will be created if the actual parameter value is a string and
        there is no such column in the table `table`. If value provided
        is an object column some sanity checks will be performed with
        the column object: it should have `nullable=False` and have
        :class:`PathField` type.

    :param depth_field='mp_depth':
        the same as for :attr:`path_field`, except that the type of this
        column should be :class:`DepthField`.

    :param tree_id_field='mp_tree_id':
        the same as for :attr:`path_field`, except that the type of this
        column should be :class:`TreeIdField`.

    :param pathlen=255:
        an integer, the length for path field. See `limits`_ for details.

    :param steplen=3:
        an integer, the number of characters in each part of the path.
        See `limits`_.

    :param instance_manager_key='_mp_instance_manager':
        name for node instance's attribute to cache node's instance
        manager.

    .. warning::
        Do not change the values of `MPManager` constructor's attributes
        after saving a first tree node. Doing this will corrupt the tree.
    """
    def __init__(self, *args, **kwargs):
        self.instance_manager_key = kwargs.pop('instance_manager_key', \
                                               '_mp_instance_manager')
        opts = MPOptions(*args, **kwargs)
        self._mp_opts = opts
        self.class_manager = None
        self.mapper_extension = MPMapperExtension(opts=opts)
        self.root_node_class = None

    def __get__(self, obj, objtype):
        """
        There may be three kinds of return values from this getter.

        The first one is used when the class which this descriptor
        is attached to is not yet mapped to any table. In that case
        the return value is an instance of :class:`MPMapperExtension`.
        which is intended to be used as mapper extension.

        The second scenario is access to :class:`MPManager` via mapped
        class. The corresponding :class:`MPClassManager` instance
        is returned.

        .. note:: If the nodes of your tree use polymorphic inheritance
                  it is important to know that class manager is accessible
                  only via the base class of inheritance hierarchy.

        And the third way is accessing it from the node instance.
        Attached to that node :class:`instance manager <MPInstanceManager>`
        is returned then.
        """
        if obj is None:
            try:
                root_node_class = self.get_root_node_class(objtype)
            except sqlalchemy.orm.exc.UnmappedClassError:
                return self.mapper_extension
            assert objtype is root_node_class, \
                   "MPClassManager should be accessed via base class in the " \
                   "polymorphic inheritance hierarchy: %r" % root_node_class
            if self.class_manager is None:
                self.class_manager = MPClassManager(objtype, self._mp_opts)
            return self.class_manager
        else:
            instance_manager = obj.__dict__.get(self.instance_manager_key)
            if instance_manager is None:
                root_node_class = self.get_root_node_class(objtype)
                instance_manager = MPInstanceManager(
                    self._mp_opts, root_node_class, obj
                )
                obj.__dict__[self.instance_manager_key] = instance_manager
            return instance_manager

    def get_root_node_class(self, objtype):
        """
        Get the root node class in the polymorphic inheritance hierarchy.
        """
        if self.root_node_class is None:
            mapper = class_mapper(objtype)
            while mapper.inherits is not None:
                mapper = mapper.inherits
            self.root_node_class = mapper.class_
        return self.root_node_class


_nonexistent = object()
def _iter_current_next(sequence):
    """
    Generate `(current, next)` tuples from sequence. Last tuple will
    have `_nonexistent` object at the second place.

    >>> x = _iter_current_next('1234')
    >>> next(x), next(x), next(x)
    (('1', '2'), ('2', '3'), ('3', '4'))
    >>> next(x) == ('4', _nonexistent)
    True
    >>> list(_iter_current_next(''))
    []
    >>> list(_iter_current_next('1')) == [('1', _nonexistent)]
    True
    """
    iterator = iter(sequence)
    current_item = next(iterator)
    while current_item != _nonexistent:
        try:
            next_item = next(iterator)
        except StopIteration:
            next_item = _nonexistent
        yield (current_item, next_item)
        current_item = next_item

def _recursive_iterator(sequence, is_child_func):
    """
    Make a recursive iterator from plain sequence using :attr:`is_child_func`
    to determine parent-children relations. Works right only if used in
    depth-first recursive consumer.

    :param is_child_func:
        a callable object which accepts two positional arguments and
        returns `True` value if first argument value is parent of second
        argument value.

    >>> is_child_func = lambda parent, child: child > parent
    >>> def listify(seq):
    ...     return [(node, listify(children)) for node, children in seq]
    >>> listify(_recursive_iterator('ABCABB', is_child_func))
    [('A', [('B', [('C', [])])]), ('A', [('B', []), ('B', [])])]
    >>> listify(_recursive_iterator('', is_child_func))
    []
    >>> next(_recursive_iterator('A', is_child_func))
    ('A', ())
    >>> next(_recursive_iterator('AB', is_child_func)) # doctest: +ELLIPSIS
    ('A', <generator object ...>)
    """
    current_next_iterator = _iter_current_next(sequence)
    item = {}
    is_parent_of_next = lambda node: \
            item['next'] is not _nonexistent \
            and is_child_func(node, item['next'])

    def step():
        item['current'], item['next'] = next(current_next_iterator)
        if is_parent_of_next(item['current']):
            return (item['current'], children_generator(item['current']))
        else:
            return (item['current'], tuple())

    def children_generator(parent_node):
        while True:
            yield step()
            if not is_parent_of_next(parent_node):
                break

    while True:
        yield step()


def tree_recursive_iterator(flat_tree, class_manager):
    """
    Make a recursive iterator from plain tree nodes sequence (`Query`
    instance for example). Generates two-item tuples: node itself
    and it's children collection (which also generates two-item tuples...)
    Children collection evaluates to ``False`` if node has no children
    (it is zero-length tuple for leaf nodes), else it is a generator object.

    :param flat_tree: plain sequence of tree nodes.
    :param class_manager: instance of :class:`MPClassManager`.

    Can be used when it is simpler to process tree structure recursively.
    Simple usage example::

        def recursive_tree_processor(nodes):
            print '<ul>'
            for node, children in nodes:
                print '<li>%s' % node.name,
                if children:
                    recursive_tree_processor(children)
                print '</li>'
            print '</ul>'

        query = root_node.mp.query_descendants(and_self=True)
        recursive_tree_processor(
            sqlamp.tree_recursive_iterator(query, Node.mp)
        )

    .. versionchanged::
        0.6
        Before this function was sorting `flat_tree` if it was a query-object.
        Since 0.6 it doesn't do it, so make sure that `flat_tree` is properly
        sorted. The best way to achieve this is using queries returned from
        public API methods of :class:`MPClassManager` and
        :class:`MPInstanceManager`.

    .. warning:: Process `flat_tree` items once and sequentially so works
      right only if used in depth-first recursive consumer.
    """
    opts = class_manager._mp_opts
    tree_id = attrgetter(opts.tree_id_field.name)
    depth = attrgetter(opts.depth_field.name)
    def is_child(parent, child):
        return tree_id(parent) == tree_id(child) \
                and depth(child) == depth(parent) + 1
    return _recursive_iterator(flat_tree, is_child)


class DeclarativeMeta(BaseDeclarativeMeta):
    """
    Metaclass for declaratively defined node model classes.

    .. versionadded:: 0.5

    See :ref:`usage example <declarative>` above in Quickstart.

    All options that accepts :class:`MPManager` can be provided
    with declarative definition. To provide an option you can
    simply assign value to class' property with name like
    ``__mp_tree_id_field__`` (for ``tree_id_field`` parameter)
    and so forth. See the complete list of options in :class:`MPManager`'s
    constructor parameters. Note that you can use only string options
    for field names, not the column objects.

    A special class variable ``__mp_manager__`` should exist and hold
    a string name which will be used as `MPManager` descriptor property.
    """
    def __init__(cls, name, bases, dct):
        if not hasattr(cls, '__mp_manager__'):
            super(DeclarativeMeta, cls).__init__(name, bases, dct)
            return
        mp_manager_name = cls.__mp_manager__

        # preventing the property from being inherited
        del cls.__mp_manager__

        # After this step mapper and table are created.
        super(DeclarativeMeta, cls).__init__(name, bases, dct)

        opts = {}
        for opt in ['path_field', 'depth_field', 'tree_id_field',
                    'steplen', 'pathlen', 'instance_manager_key']:
            optname = '__mp_%s__' % opt
            if hasattr(cls, optname):
                opts[opt] = getattr(cls, optname)
                delattr(cls, optname)

        # Suppressing attaching columns to the table, because
        # those will be attached when we set the class' property
        mp_manager = MPManager(cls.__table__, _attach_columns=False, **opts)
        for field in mp_manager._mp_opts.fields:
            # Columns get attached to the table here
            if not hasattr(cls, field.name):
                # SQLAlchemy 0.5.x needs this:
                dct[field.name] = field
                # and SQLAlchemy 0.6.x needs this:
                setattr(cls, field.name, field)

        # Declaring indices manually, as it has to be done after
        # attaching columns to the table.
        mp_manager._mp_opts.declare_indices()

        setattr(cls, mp_manager_name, mp_manager)
        mapper_ext = mp_manager.mapper_extension
        if hasattr(cls.__mapper__, 'extension'):
            # SQLAlchemy < 0.7
            cls.__mapper__.extension.append(mapper_ext)
        else:
            # SQLAlchemy 0.7+
            from sqlalchemy import event
            event.listen(cls.__mapper__, 'before_insert',
                         mapper_ext.before_insert, propagate=True)
            event.listen(cls.__mapper__, 'after_insert',
                         mapper_ext.after_insert, propagate=True)

