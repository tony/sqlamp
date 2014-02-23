import sys
import os
import unittest

import sqlalchemy
import sqlamp


engine = None


class _BaseTestCase(unittest.TestCase):
    def setUp(self):
        setup()
        self.sess = make_session()
        if tbl.exists():
            tbl.drop()
        tbl.create()

    def tearDown(self):
        self.sess.close()
        tbl.drop()
        tbl.create()


def setup():
    global engine, make_session, tbl, Cls, metadata

    if engine is not None:
        # already set up
        return

    if not 'DB_URI' in os.environ:
        error_msg = "Set environment variable `DB_URI'.\n"
        sys.exit(error_msg)

    DB_URI = os.environ['DB_URI']
    ECHO = bool(os.environ.get('ECHO', False))
    MYSQL_ENGINE = os.environ.get('MYSQL_ENGINE', 'MyISAM')

    engine = sqlalchemy.create_engine(DB_URI, echo=ECHO)

    make_session = sqlalchemy.orm.sessionmaker(
        bind=engine, autocommit=False, autoflush=True
    )

    metadata = sqlalchemy.MetaData(engine)

    # mysql still doesn't support deferring of fk constraints (though it is
    # required by SQL standard) on deleting. So for mysql we need to make it
    # cascade delete queries in order to make deleting subtrees work. See
    # http://dev.mysql.com/doc/refman/5.5/en/innodb-foreign-key-constraints.html
    ondelete = DB_URI.startswith('mysql') and 'CASCADE' or 'RESTRICT'

    tbl = sqlalchemy.Table('tbl', metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,),
        sqlalchemy.Column('name', sqlalchemy.String(100)),
        sqlalchemy.Column('parent_id',
                          sqlalchemy.ForeignKey('tbl.id', ondelete=ondelete)),
        mysql_engine=MYSQL_ENGINE
    )

    class Cls(object):
        mp = sqlamp.MPManager(tbl, steplen=2)

        def __init__(self, **kwargs):
            for key, val in kwargs.items():
                setattr(self, key, val)

        def __repr__(self):
            return '<Cls %s(%s): parent=%s, path=%s, depth=%s>' % \
                   (self.name, self.id, self.parent_id,
                    self.mp_path, self.mp_depth)


    sqlalchemy.orm.mapper(
        Cls, tbl, extension=[Cls.mp],
        properties={
            'parent': sqlalchemy.orm.relation(Cls, remote_side=[tbl.c.id])
        }
    )

    if tbl.exists():
        tbl.drop()
    tbl.create()

