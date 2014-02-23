#!/usr/bin/env python
"""
`sqlamp` functional tests.
"""
import random
import unittest
import pickle

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import sqlamp

import tests._testlib as _testlib
_testlib.setup()
from tests._testlib import Cls, make_session, tbl, metadata


class _BaseFunctionalTestCase(_testlib._BaseTestCase):
    def setUp(self):
        super(_BaseFunctionalTestCase, self).setUp()
        self.name_pattern = [
            ("root1", [
                ("child11", []),
                ("child12", []),
                ("child13", []),
            ]),
            ("root2", [
                ("child21", [
                    ("child211", []),
                    ("child212", [
                        ("child2121", []),
                        ("child2122", [
                            ("child21221", []),
                            ("child21222", []),
                        ]),
                    ]),
                ]),
                ("child22", []),
                ("child23", []),
            ]),
            ("root3", []),
        ]

    def n(self, node_name):
        return Cls.mp.query(self.sess).filter_by(name=node_name).one()

    def _fill_tree(self):
        def _create_node(name, parent=None):
            node = Cls(name=name, parent=parent)
            self.sess.add(node)
            self.sess.flush()
            return node

        def _process_node(node, parent=None):
            name, children = node
            node = _create_node(name, parent)
            for child in children:
                _process_node(child, node)

        for node in self.name_pattern:
            _process_node(node)
        self.sess.commit()

    def _corrupt_tree(self, including_roots):
        root1, root2, root3 = self.sess.query(Cls).filter_by(parent=None) \
                                                  .order_by('name')
        # corrupting path:
        used_pathes = set() # remember that they should be unique
        for node in root1.mp.query_descendants():
            while True:
                path = ''.join(
                    random.sample(
                        sqlamp.ALPHABET + '!@#$%^&*', random.randint(1, 40)
                    )
                )
                if not path in used_pathes:
                    used_pathes.add(path)
                    break
            node.mp_path = path
        if including_roots:
            root1.mp_path = '[][][]'
        # depth:
        for node in root2.mp.query_descendants():
            node.mp_depth = random.randint(10, 90)
        if including_roots:
            root2.mp_depth = 100
        # tree_id:
        for node in root3.mp.query_descendants():
            node.mp_tree_id = _from = random.randint(1, 2000)
        if including_roots:
            root3.mp_tree_id = 42 + root3.mp_tree_id

        self.sess.commit()
        self.sess.expire_all()
        return [root1, root2, root3]


class InsertsTestCase(_BaseFunctionalTestCase):
    def test_insert_roots(self):
        o, o2, o3 = Cls(), Cls(), Cls()
        self.sess.add_all([o, o2, o3])
        self.sess.commit()
        self.assertTrue(o2 in self.sess)
        self.assertFalse(o2 in self.sess.dirty)

        o1, o2, o3 = self.sess.query(Cls).order_by('id').all()
        self.assertEqual(o1.mp_tree_id, 1)
        self.assertEqual(o2.mp_tree_id, 2)
        self.assertEqual(o3.mp_tree_id, 3)

        for o in [o1, o2, o3]:
            self.assertEqual(o.mp_path, '')
            self.assertEqual(o.mp_depth, 0)

    def test_insert_children(self):
        parent = Cls()
        self.sess.add(parent)
        self.sess.flush()

        children = Cls(), Cls(), Cls()
        for child in children:
            child.parent = parent
        self.sess.add_all(children)
        self.sess.commit()
        self.sess.expunge_all()

        parent, c1, c2, c3 = self.sess.query(Cls).order_by('id').all()
        for child in [c1, c2, c3]:
            self.assertEqual(child.mp_tree_id, parent.mp_tree_id)
            self.assertEqual(child.mp_depth, 1)
        self.assertEqual(c1.mp_path, '00')
        self.assertEqual(c2.mp_path, '01')
        self.assertEqual(c3.mp_path, '02')

        children = Cls(), Cls(), Cls()
        for child in children:
            child.parent = c1
        self.sess.add_all(children)
        self.sess.commit()

        for child in children:
            self.assertEqual(child.mp_tree_id, parent.mp_tree_id)
            self.assertEqual(child.mp_depth, 2)
        self.assertEqual(children[0].mp_path, c1.mp_path + '00')
        self.assertEqual(children[1].mp_path, c1.mp_path + '01')
        self.assertEqual(children[2].mp_path, c1.mp_path + '02')


class RebuildTestCase(_BaseFunctionalTestCase):
    def test_rebuild_all_trees(self):
        self._fill_tree()
        query = sqlalchemy.select([tbl]).order_by(tbl.c.id)
        data_before = query.execute().fetchall()
        self._corrupt_tree(including_roots=True)
        # rebuilding all trees:
        Cls.mp.rebuild_all_trees(self.sess)
        # all trees should be in consistent state
        # and be absolutely the same as before corruption.
        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

    def test_drop_indices(self):
        Cls.mp.drop_indices(self.sess)
        [index] =Cls.mp._mp_opts.indices
        # creating index again should succeed
        index.create()

    def test_create_indices(self):
        [index] =Cls.mp._mp_opts.indices
        index.drop()
        Cls.mp.create_indices(self.sess)
        # dropping index again should succeed
        index.drop()


class QueriesTestCase(_BaseFunctionalTestCase):
    def test_descendants(self):
        self._fill_tree()
        child212 = self.n('child212')
        descendants = Cls.mp.query(self.sess).filter(
            child212.mp.filter_descendants(and_self=False)
        ).all()
        self.assertEqual(descendants, child212.mp.query_descendants().all())
        should_be = Cls.mp.query(self.sess).filter(
            tbl.c.name.in_(
                ("child2121", "child2122", "child21221", "child21222")
            )
        ).all()
        self.assertEqual(descendants, should_be)
        descendants_and_self = Cls.mp.query(self.sess).filter(
            child212.mp.filter_descendants(and_self=True)
        ).all()
        self.assertEqual(
            descendants_and_self,
            child212.mp.query_descendants(and_self=True).all()
        )
        self.assertEqual(descendants_and_self, [child212] + should_be)

    def test_children(self):
        self._fill_tree()
        root2 = self.n('root2')
        children = Cls.mp.query(self.sess).filter(
            root2.mp.filter_children()
        ).all()
        should_be = Cls.mp.query(self.sess).filter(
            tbl.c.name.in_(("child21", "child22", "child23"))
        ).all()
        self.assertEqual(children, should_be)
        self.assertEqual(children, root2.mp.query_children().all())

    def test_filter_parent(self):
        self._fill_tree()
        root1 = self.n('root1')
        self.assertEqual(
            self.sess.query(Cls).filter(root1.mp.filter_parent()).count(), 0
        )
        for child in root1.mp.query_children():
            self.assertEqual(
                self.sess.query(Cls).filter(child.mp.filter_parent()).one(),
                root1
            )

    def test_ancestors(self):
        self._fill_tree()
        child2122 = self.n('child2122')
        ancestors = Cls.mp.query(self.sess).filter(
            child2122.mp.filter_ancestors()
        ).all()
        should_be = Cls.mp.query(self.sess).filter(
            tbl.c.name.in_(("child212", "child21", "child2", "root2"))
        ).all()
        self.assertEqual(ancestors, should_be)
        self.assertEqual(ancestors, child2122.mp.query_ancestors().all())
        ancestors_and_self = Cls.mp.query(self.sess).filter(
            child2122.mp.filter_ancestors(and_self=True)
        ).all()
        self.assertEqual(ancestors_and_self, should_be + [child2122])
        self.assertEqual(
            ancestors_and_self,
            child2122.mp.query_ancestors(and_self=True).all()
        )

    def test_query_all_trees(self):
        self._fill_tree()
        all_trees = Cls.mp.query_all_trees(self.sess)
        self.assertEqual(
            [node.name for node in all_trees],
            ["root1", "child11", "child12", "child13", "root2", "child21",
             "child211", "child212", "child2121", "child2122",
             "child21221", "child21222", "child22", "child23", "root3"]
        )

    def test_tree_recursive_iterator(self):
        self._fill_tree()
        all_trees = Cls.mp.query_all_trees(self.sess)
        all_trees = sqlamp.tree_recursive_iterator(all_trees, Cls.mp)
        def listify(recursive_iterator):
            return [(node.name, listify(children))
                    for node, children in recursive_iterator]
        self.assertEqual(self.name_pattern, listify(all_trees))


class LimitsTestCase(_BaseFunctionalTestCase):
    def test_too_many_children_and_last_child_descendants(self):
        self.assertEqual(Cls.mp.max_children, 1296) # 36 ** 2
        root = Cls()
        self.sess.add(root)
        self.sess.commit()
        for x in range(1296):
            self.sess.add(Cls(parent=root, name=str(x)))
        self.sess.commit()
        self.sess.add(Cls(parent=root))
        self.assertRaises(sqlamp.TooManyChildrenError, self.sess.flush)
        self.sess.rollback()
        last_child = self.n('1295')
        last_childs_child = Cls(parent=last_child, name='1295.1')
        self.sess.add(last_childs_child)
        self.sess.flush()
        self.assertEqual(
            last_child.mp.query_descendants().all(), [last_childs_child]
        )

    def test_path_too_deep(self):
        self.assertEqual(Cls.mp.max_depth, 128) # int(255 / 2) + 1
        node = None
        for x in range(128):
            new_node = Cls(parent=node)
            self.sess.add(new_node)
            self.sess.flush()
            node = new_node
        self.sess.add(Cls(parent=node))
        self.assertRaises(sqlamp.PathTooDeepError, self.sess.flush)


class SetupTestCase(_BaseFunctionalTestCase):
    def test_declarative(self):
        BaseNode = declarative_base(metadata=metadata, \
                                    metaclass=sqlamp.DeclarativeMeta)
        class Node(BaseNode):
            __tablename__ = 'node'
            __mp_manager__ = 'MP'
            __mp_steplen__ = 5
            __mp_depth_field__ = 'MP_depth'
            __mp_path_field__ = 'path'
            path = sqlalchemy.Column(sqlamp.PathField(length=120),
                                     nullable=False)
            id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
            parent_id = sqlalchemy.Column(sqlalchemy.ForeignKey('node.id'))
            parent = sqlalchemy.orm.relation("Node", remote_side=[id])
            name = sqlalchemy.Column(sqlalchemy.String(100))

        self.assertEqual(Node.MP._mp_opts.pathlen, 120)
        self.assertEqual(Node.MP._mp_opts.steplen, 5)
        self.assertEqual(Node.MP._mp_opts.path_field, Node.__table__.c.path)
        self.assertEqual(Node.MP._mp_opts.depth_field.name, 'MP_depth')

        if Node.__table__.exists():
            Node.__table__.drop()
        Node.__table__.create()

        try:
            root = Node()
            self.assert_(isinstance(root.MP, sqlamp.MPInstanceManager))
            self.sess.add(root)
            self.sess.commit()
            child = Node()
            child.parent = root
            self.sess.add(child)
            self.sess.commit()

            [root, child] = self.sess.query(Node).order_by('id').all()
            self.assertEqual(root.path, '')
            self.assertEqual(root.MP_depth, 0)
            self.assertEqual(child.path, '00000')
            self.assertEqual(child.MP_depth, 1)
        finally:
            Node.__table__.delete()

    def test_implicit_pk_fk(self):
        tbl = sqlalchemy.Table('tbl2', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid', sqlalchemy.ForeignKey('tbl2.id'))
        )
        mpm = sqlamp.MPManager(tbl)
        self.assertEqual(tbl.c.id, mpm._mp_opts.pk_field)
        self.assertEqual(tbl.c.pid, mpm._mp_opts.parent_id_field)

    def test_more_than_one_backref(self):
        tbl = sqlalchemy.Table('tbl3', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid1', sqlalchemy.ForeignKey('tbl3.id')),
            sqlalchemy.Column('pid2', sqlalchemy.ForeignKey('tbl3.id')),
            sqlalchemy.Column('pid3', sqlalchemy.ForeignKey('tbl3.id'))
        )
        mpm = sqlamp.MPManager(tbl, parent_id_field='pid2')
        self.assertEqual(mpm._mp_opts.parent_id_field, tbl.c.pid2)

    def test_custom_pathlen(self):
        tbl = sqlalchemy.Table('tbl4', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid', sqlalchemy.ForeignKey('tbl4.id'))
        )
        mpm = sqlamp.MPManager(tbl, steplen=1, pathlen=512)
        self.assertEqual(tbl.c.mp_path.type.impl.length, 512)

    def test_custom_column_and_same_column_options(self):
        tbl = sqlalchemy.Table('tbl5', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid', sqlalchemy.ForeignKey('tbl5.id')),
            sqlalchemy.Column('path', sqlamp.PathField(length=40),
                              nullable=False)
        )
        mpm = sqlamp.MPManager(tbl, pathlen=40)
        self.assertEqual(tbl.c.mp_path.type.impl.length, 40)

    def test_custom_column_and_different_column_options(self):
        tbl = sqlalchemy.Table('tbl6', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid', sqlalchemy.ForeignKey('tbl6.id')),
        )
        path_field = sqlalchemy.Column('path', sqlamp.PathField(length=50),
                                       nullable=False)
        self.assertRaises(AssertionError, sqlamp.MPManager, tbl,
                          pathlen=60, path_field=path_field)

    def test_table_inheritance(self):
        tbl_abstract = sqlalchemy.Table('tbl_abstract', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('parent_id',
                              sqlalchemy.ForeignKey('tbl_abstract.id')),
            sqlalchemy.Column('type', sqlalchemy.String(100), nullable=False)
        )
        tbl_sub = sqlalchemy.Table('tbl_sub', metadata,
            sqlalchemy.Column('id', sqlalchemy.ForeignKey('tbl_abstract.id'),
                              primary_key=True)
        )

        class AbstractNode(object):
            mp = sqlamp.MPManager(tbl_abstract)
        sqlalchemy.orm.mapper(
            AbstractNode, tbl_abstract, polymorphic_on=tbl_abstract.c.type,
            polymorphic_identity='abstract',
            extension=[AbstractNode.mp]
        )

        class SubNode(AbstractNode):
            pass
        sqlalchemy.orm.mapper(SubNode, tbl_sub, inherits=AbstractNode,
                              polymorphic_identity='sub')

        if tbl_sub.exists():
            tbl_sub.drop()
        if tbl_abstract.exists():
            tbl_abstract.drop()
        tbl_abstract.create()
        tbl_sub.create()

        try:
            abstract_node = AbstractNode()
            self.sess.add(abstract_node); self.sess.commit()
            sub_node = SubNode()
            sub_node.parent_id = abstract_node.id
            self.sess.add(sub_node); self.sess.commit()
            [a, s] = AbstractNode.mp.query_all_trees(self.sess)
            self.assert_(a is abstract_node)
            self.assert_(s is sub_node)
        finally:
            tbl_abstract.delete()
            tbl_sub.delete()

    def test_table_inheritance_declarative(self):
        BaseNode = declarative_base(metadata=metadata, \
                                    metaclass=sqlamp.DeclarativeMeta)
        class AbstractNode(BaseNode):
            __tablename__ = 'node2'
            __mp_manager__ = 'mp'
            id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
            parent_id = sqlalchemy.Column(sqlalchemy.ForeignKey('node2.id'))
            type = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
            __mapper_args__ = {'polymorphic_on': type,
                               'polymorphic_identity': 'abs'}
        class SubNode(AbstractNode):
            __tablename__ = 'node2sub'
            __mapper_args__ = {'polymorphic_identity': 'sub'}
            id = sqlalchemy.Column(sqlalchemy.ForeignKey('node2.id'),
                                   primary_key=True)
        class SubNode2(AbstractNode):
            __tablename__ = 'node2sub2'
            __mapper_args__ = {'polymorphic_identity': 'sub2'}
            id = sqlalchemy.Column(sqlalchemy.ForeignKey('node2.id'),
                                   primary_key=True)

        tables = AbstractNode.__table__, SubNode.__table__, SubNode2.__table__
        for table in reversed(tables):
            if table.exists():
                table.drop()
        for table in tables:
            table.create()

        try:
            abstract_node = AbstractNode()
            self.sess.add(abstract_node); self.sess.commit()

            sub_node = SubNode()
            sub_node.parent_id = abstract_node.id
            self.sess.add(sub_node); self.sess.commit()

            sub_node2 = SubNode2()
            sub_node2.parent_id = sub_node.id
            self.sess.add(sub_node2); self.sess.commit()

            [a, s, s2] = AbstractNode.mp.query_all_trees(self.sess)
            self.assert_(a is abstract_node)
            self.assert_(s is sub_node)
            self.assert_(s2 is sub_node2)

            [s2] = s.mp.query_descendants()
            self.assert_(s2 is sub_node2)
        finally:
            AbstractNode.__table__.delete()
            SubNode.__table__.delete()
            SubNode2.__table__.delete()


class PickleTestCase(_BaseFunctionalTestCase):
    def test_pickle_node(self):
        parent = Cls(name='parent')
        child = Cls(name='child', parent=parent)
        self.sess.add_all([parent, child])
        self.sess.flush()
        # making instance managers to appear
        parent.mp, child.mp
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            for origin in (parent, child):
                clone = pickle.loads(pickle.dumps(origin, protocol))
                self.assertEqual(clone.mp._mp_opts, origin.mp._mp_opts)
                self.assertEqual(clone.mp._root_node_class,
                                 origin.mp._root_node_class)
                self.assertEqual(clone.mp._obj_ref().name,
                                 origin.mp._obj_ref().name)
                for meth in ('query_children', 'query_descendants',
                             'query_ancestors'):
                    self.assertEqual(getattr(clone.mp, meth)(self.sess).all(),
                                     getattr(origin.mp, meth)(self.sess).all())


class MoveNodesTestCase(_BaseFunctionalTestCase):
    def test_detach_subtree(self):
        self._fill_tree()
        child212 = self.n('child212')

        Cls.mp.detach_subtree(self.sess, child212.id)
        self.sess.expunge_all()

        child212 = self.n('child212')
        former_parent = self.n('child21')
        new_children = [n.name for n in former_parent.mp.query_descendants()]
        self.assertEqual(new_children, ['child211'])
        self.assertEqual(child212.mp_path, '')
        self.assertEqual(child212.mp_depth, 0)
        self.assertNotEqual(child212.mp_tree_id, former_parent.mp_tree_id)
        nodes_children = [(n.name, n.mp_path)
                          for n in child212.mp.query_descendants()]
        self.assertEqual(
            nodes_children,
            [('child2121', '00'), ('child2122', '01'),
             ('child21221', '0100'), ('child21222', '0101')]
        )

    def test_detach_subtree_gaps(self):
        self.name_pattern = [
            ("root", [
                ("child1", []),
                ("child2", [
                    ("child21", []),
                    ("child22", [
                        ("child221", []),
                        ("child222", []),
                    ]),
                ]),
                ("child3", []),
            ]),
        ]
        self._fill_tree()

        Cls.mp.detach_subtree(self.sess, self.n('child1').id)

        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ("root", "", 0),
              ("child2", "00", 1),
                ("child21", "0000", 2),
                ("child22", "0001", 2),
                  ("child221", "000100", 3),
                  ("child222", "000101", 3),
              ("child3", "01", 1),
            ("child1", "", 0)
        ])

        Cls.mp.detach_subtree(self.sess, self.n('child21').id)
        self.sess.expunge_all()

        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ("root", "", 0),
              ("child2", "00", 1),
                ("child22", "0000", 2),
                  ("child221", "000000", 3),
                  ("child222", "000001", 3),
              ("child3", "01", 1),
            ("child1", "", 0),
            ("child21", "", 0),
        ])

    def test_move_subtree_before(self):
        self._fill_tree()
        node_id, anchor_id = self.n('child2122').id, self.n('child212').id
        Cls.mp.move_subtree_before(self.sess, node_id, anchor_id=anchor_id)
        self.sess.expunge_all()
        new_parent = self.n('child21')
        nodes_children = [(n.name, n.mp_path, n.mp_depth)
                          for n in new_parent.mp.query_descendants()]
        self.assertEqual(nodes_children, [
            ('child211', '0000', 2),
            ('child2122', '0001', 2),
              ('child21221', '000100', 3),
              ('child21222', '000101', 3),
            ('child212', '0002', 2),
              ('child2121', '000200', 3)
        ])

    def test_move_subtree_before_first_node(self):
        self._fill_tree()
        node_id, anchor_id = self.n('child12').id, self.n('child11').id
        Cls.mp.move_subtree_before(self.sess, node_id, anchor_id=anchor_id)
        self.sess.expunge_all()
        new_parent = self.n('root1')
        nodes_children = [(n.name, n.mp_path, n.mp_depth)
                          for n in new_parent.mp.query_descendants()]
        self.assertEqual(nodes_children, [
            ('child12', '00', 1),
            ('child11', '01', 1),
            ('child13', '02', 1),
        ])

    def test_move_subtree_before_gaps(self):
        self._fill_tree()
        Cls.mp.move_subtree_before(self.sess, self.n('child211').id,
                                   anchor_id=self.n('child21').id)
        self.sess.expunge_all()
        new_parent = self.n('root2')
        nodes_children = [(n.name, n.mp_path, n.mp_depth)
                          for n in new_parent.mp.query_descendants()]
        self.assertEqual(nodes_children, [
            ('child211', '00', 1),
            ('child21', '01', 1),
              ('child212', '0100', 2),
                ('child2121', '010000', 3),
                ('child2122', '010001', 3),
                  ('child21221', '01000100', 4),
                  ('child21222', '01000101', 4),
            ('child22', '02', 1),
            ('child23', '03', 1),
        ])

    def test_move_subtree_to_descendant(self):
        self._fill_tree()
        query = sqlalchemy.select([tbl]).order_by(tbl.c.id)
        data_before = query.execute().fetchall()
        # moving before
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_before, self.sess,
                          self.n('child21').id, self.n('child211').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_before, self.sess,
                          self.n('root2').id, self.n('child212').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_before, self.sess,
                          self.n('child21').id, self.n('child21222').id)
        # moving after
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_after, self.sess,
                          self.n('child21').id, self.n('child211').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_after, self.sess,
                          self.n('root2').id, self.n('child212').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_after, self.sess,
                          self.n('child21').id, self.n('child21222').id)
        # moving to top
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_top, self.sess,
                          self.n('child21').id, self.n('child211').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_top, self.sess,
                          self.n('root2').id, self.n('child212').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_top, self.sess,
                          self.n('child21').id, self.n('child21222').id)
        # moving to bottom
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_bottom, self.sess,
                          self.n('child21').id, self.n('child211').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_bottom, self.sess,
                          self.n('root2').id, self.n('child212').id)
        self.assertRaises(sqlamp.MovingToDescendantError,
                          Cls.mp.move_subtree_to_bottom, self.sess,
                          self.n('child21').id, self.n('child21222').id)
        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

    def test_move_subtree_before_same_parent(self):
        self.name_pattern = [
            ("root", [
                ("child1", [
                    ("child11", []),
                ]),
                ("child2", [
                    ("child21", []),
                ]),
                ("child3", [
                    ("child31", []),
                ]),
                ("child4", [
                    ("child41", []),
                ]),
            ]),
        ]
        self._fill_tree()

        Cls.mp.move_subtree_before(self.sess, self.n('child3').id,
                                   anchor_id=self.n('child2').id)
        self.sess.expunge_all()
        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ('root', '', 0),
              ('child1', '00', 1),
                ('child11', '0000', 2),
              ('child3', '01', 1),
                ('child31', '0100', 2),
              ('child2', '02', 1),
                ('child21', '0200', 2),
              ('child4', '03', 1),
                ('child41', '0300', 2),
        ])

        Cls.mp.move_subtree_before(self.sess, self.n('child1').id,
                                   anchor_id=self.n('child4').id)
        self.sess.expunge_all()
        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ('root', '', 0),
              ('child3', '00', 1),
                ('child31', '0000', 2),
              ('child2', '01', 1),
                ('child21', '0100', 2),
              ('child1', '02', 1),
                ('child11', '0200', 2),
              ('child4', '03', 1),
                ('child41', '0300', 2),
        ])

    def test_move_subtree_before_moving_root(self):
        self._fill_tree()
        Cls.mp.move_subtree_before(self.sess, self.n('root1').id,
                                   anchor_id=self.n('child212').id)
        self.sess.expunge_all()
        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ('root2', '', 0),
              ('child21', '00', 1),
                ('child211', '0000', 2),
                ('root1', '0001', 2),
                  ('child11', '000100', 3),
                  ('child12', '000101', 3),
                  ('child13', '000102', 3),
                ('child212', '0002', 2),
                  ('child2121', '000200', 3),
                  ('child2122', '000201', 3),
                    ('child21221', '00020100', 4),
                    ('child21222', '00020101', 4),
              ('child22', '01', 1),
              ('child23', '02', 1),
            ('root3', '', 0),
        ])

    def test_move_subtree_after(self):
        self._fill_tree()
        Cls.mp.move_subtree_after(self.sess, self.n('child211').id,
                                  anchor_id=self.n('child2121').id)
        self.sess.expunge_all()
        child21 = self.n('child21')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child21.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child212', '0000', 2),
              ('child2121', '000000', 3),
              ('child211', '000001', 3),
              ('child2122', '000002', 3),
                ('child21221', '00000200', 4),
                ('child21222', '00000201', 4),
        ])

    def test_move_subtree_after_last_node(self):
        self._fill_tree()
        Cls.mp.move_subtree_after(self.sess, self.n('child211').id,
                                  anchor_id=self.n('child21222').id)
        self.sess.expunge_all()
        child21 = self.n('child21')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child21.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child212', '0000', 2),
              ('child2121', '000000', 3),
              ('child2122', '000001', 3),
                ('child21221', '00000100', 4),
                ('child21222', '00000101', 4),
                ('child211', '00000102', 4),
        ])

    def test_move_subtree_to_top(self):
        self._fill_tree()
        Cls.mp.move_subtree_to_top(self.sess, self.n('root1').id,
                                   new_parent_id=self.n('child21').id)
        self.sess.expunge_all()
        child21 = self.n('child21')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child21.mp.query_descendants()]
        self.assertEqual(tree, [
            ('root1', '0000', 2),
              ('child11', '000000', 3),
              ('child12', '000001', 3),
              ('child13', '000002', 3),
            ('child211', '0001', 2),
            ('child212', '0002', 2),
              ('child2121', '000200', 3),
              ('child2122', '000201', 3),
                ('child21221', '00020100', 4),
                ('child21222', '00020101', 4),
        ])

    def test_move_subtree_to_top_empty_root(self):
        self._fill_tree()
        Cls.mp.move_subtree_to_top(self.sess, self.n('child2122').id,
                                   new_parent_id=self.n('child22').id)
        self.sess.expunge_all()
        child22 = self.n('child22')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child22.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child2122', '0100', 2),
              ('child21221', '010000', 3),
              ('child21222', '010001', 3),
        ])

    def test_move_subtree_to_top_same_parent(self):
        self.name_pattern = [
            ("root", [
                ("child1", [
                    ("child11", []),
                ]),
                ("child2", [
                    ("child21", []),
                ]),
            ]),
        ]
        self._fill_tree()
        query = sqlalchemy.select([tbl]).order_by(tbl.c.id)
        data_before = query.execute().fetchall()

        Cls.mp.move_subtree_to_top(self.sess, self.n('child2').id,
                                   new_parent_id=self.n('root').id)
        self.sess.expunge_all()
        all_nodes = [(n.name, n.mp_path, n.mp_depth)
                     for n in Cls.mp.query(self.sess)]
        self.assertEqual(all_nodes, [
            ('root', '', 0),
              ('child2', '00', 1),
                ('child21', '0000', 2),
              ('child1', '01', 1),
                ('child11', '0100', 2),
        ])
        Cls.mp.move_subtree_to_top(self.sess, self.n('child1').id,
                                   new_parent_id=self.n('root').id)
        self.sess.expunge_all()
        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

    def test_move_subtree_to_bottom(self):
        self._fill_tree()
        Cls.mp.move_subtree_to_bottom(self.sess, self.n('child2122').id,
                                      new_parent_id=self.n('root1').id)
        self.sess.expunge_all()
        root1 = self.n('root1')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in root1.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child11', '00', 1),
            ('child12', '01', 1),
            ('child13', '02', 1),
            ('child2122', '03', 1),
              ('child21221', '0300', 2),
              ('child21222', '0301', 2),
        ])

    def test_move_subtree_to_bottom_empty_root(self):
        self._fill_tree()
        Cls.mp.move_subtree_to_bottom(self.sess, self.n('root1').id,
                                      new_parent_id=self.n('root3').id)
        self.sess.expunge_all()
        root3 = self.n('root3')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in root3.mp.query_descendants()]
        self.assertEqual(tree, [
            ('root1', '00', 1),
              ('child11', '0000', 2),
              ('child12', '0001', 2),
              ('child13', '0002', 2),
        ])

    def test_everything(self):
        self._fill_tree()
        columns = [tbl.c.id, tbl.c.name, tbl.c.parent_id,
                   tbl.c.mp_path, tbl.c.mp_depth]
        query = sqlalchemy.select(columns).order_by(tbl.c.id)
        data_before = query.execute().fetchall()

        detach = lambda *args: Cls.mp.detach_subtree(self.sess, *args)
        before = lambda *args: Cls.mp.move_subtree_before(self.sess, *args)
        after = lambda *args: Cls.mp.move_subtree_after(self.sess, *args)
        top = lambda *args: Cls.mp.move_subtree_to_top(self.sess, *args)
        bottom = lambda *args: Cls.mp.move_subtree_to_bottom(self.sess, *args)
        n = lambda name: self.n(name).id

        # taking apart
        detach(n('child212'))
        before(n('child23'), n('child21'))
        after(n('child11'), n('child13'))
        top(n('root1'), n('child22'))
        bottom(n('root3'), n('child22'))

        # putting together
        after(n('child212'), n('child211'))
        bottom(n('child23'), n('root2'))
        before(n('child11'), n('child12'))
        detach(n('root1'))
        detach(n('root3'))

        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

    def test_delete_subtree(self):
        self._fill_tree()
        Cls.mp.delete_subtree(self.sess, self.n('child212').id)
        self.sess.expunge_all()
        child21 = self.n('child21')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child21.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child211', '0000', 2),
        ])

    def test_delete_subtree_gaps(self):
        self._fill_tree()
        Cls.mp.delete_subtree(self.sess, self.n('child211').id)
        self.sess.expunge_all()
        child21 = self.n('child21')
        tree = [(n.name, n.mp_path, n.mp_depth)
                for n in child21.mp.query_descendants()]
        self.assertEqual(tree, [
            ('child212', '0000', 2),
              ('child2121', '000000', 3),
              ('child2122', '000001', 3),
                ('child21221', '00000100', 4),
                ('child21222', '00000101', 4),
        ])


class MoveNodesLimitsTestCase(_BaseFunctionalTestCase):
    def setUp(self):
        super(MoveNodesLimitsTestCase, self).setUp()
        self.tbl = sqlalchemy.Table('tbl7', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('pid', sqlalchemy.ForeignKey('tbl7.id'))
        )
        class Node(Cls):
            mp = sqlamp.MPManager(self.tbl, steplen=1)
        rel = sqlalchemy.orm.relation(Node, remote_side=[self.tbl.c.id])
        sqlalchemy.orm.mapper(Node, self.tbl, extension=[Node.mp],
                              properties={'parent': rel})
        self.Node = Node
        self.tbl.create()

        # create two trees with roots packed of children up to limit:
        self.r1 = self.Node()
        self.r2 = self.Node()
        for root in (self.r1, self.r2):
            self.sess.add(root)
            self.sess.flush()
            for i in range(len(sqlamp.ALPHABET)):
                self.sess.add(self.Node(pid=root.id))
        self.sess.commit()
        self.sess.expire_all()

    def tearDown(self):
        super(MoveNodesLimitsTestCase, self).tearDown()
        self.tbl.drop()
        metadata.remove(self.tbl)

    def test_move_by_sibling(self):
        query = sqlalchemy.select([self.tbl]).order_by(self.tbl.c.id)
        data_before = query.execute().fetchall()

        c12 = self.r1.mp.query_children()[1]
        self.assertRaises(sqlamp.TooManyChildrenError,
                          self.Node.mp.move_subtree_before,
                          self.sess, self.r2.id, c12.id)
        self.assertRaises(sqlamp.TooManyChildrenError,
                          self.Node.mp.move_subtree_after,
                          self.sess, self.r2.id, c12.id)

        # nothing should have been changed so far
        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

        # free some space
        self.Node.mp.delete_subtree(self.sess, c12.id)
        self.sess.expire_all()

        # now we should be able to insert something
        c13 = self.r1.mp.query_children()[1]
        self.Node.mp.move_subtree_after(self.sess, self.r2.id, c13.id)

    def test_move_by_parent(self):
        query = sqlalchemy.select([self.tbl]).order_by(self.tbl.c.id)
        data_before = query.execute().fetchall()

        self.assertRaises(sqlamp.TooManyChildrenError,
                          self.Node.mp.move_subtree_to_top,
                          self.sess, self.r2.id, self.r1.id)
        self.assertRaises(sqlamp.TooManyChildrenError,
                          self.Node.mp.move_subtree_to_bottom,
                          self.sess, self.r1.id, self.r2.id)

        data_after = query.execute().fetchall()
        self.assertEqual(data_before, data_after)

        c19 = self.r1.mp.query_children()[8]
        self.Node.mp.delete_subtree(self.sess, c19.id)
        self.sess.expire_all()

        self.Node.mp.move_subtree_to_top(self.sess, self.r2.id, self.r1.id)


def get_suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for testcase in _BaseFunctionalTestCase.__subclasses__():
        suite.addTests(loader.loadTestsFromTestCase(testcase))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(get_suite())

