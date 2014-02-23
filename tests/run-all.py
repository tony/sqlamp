#!/usr/bin/env python
"""
Test runner for those who have no nose.
"""
import unittest


TEST_MODULES = (
    'tests.benchmark-tests',
    'tests.doctests',
    'tests.functional-tests'
)

def main():
    from optparse import OptionParser

    usage = "Usage: %prog <db_uri> [options]"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-b", "--benchmark", dest="benchmark",
        help="perform benchmarks", action='store_true',
        default=False
    )
    parser.add_option(
        "--mysql-engine", dest='mysql_engine',
        help="specify mysql engine to use, can be 'MyISAM' " \
             "(default) or 'InnoDB'",
        default='MyISAM'
    )
    parser.add_option(
        "-e", "--echo", dest='echo', action='store_true',
        help="enable sqlalchemy debug output to stdout",
        default=False
    )
    parser.add_option(
        "-q", "--quiet", dest="quiet", action="store_true",
        help="do not print progress information",
        default=False
    )

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error('specify database connection string ' \
                     '(for example, "sqlite://")')

    [db_uri] = args
    import os
    os.environ['DB_URI'] = db_uri
    if options.echo:
        os.environ['ECHO'] = '1'
    if options.benchmark:
        os.environ['BENCHMARK'] = '1'
    os.environ['MYSQL_ENGINE'] = options.mysql_engine

    # checking imports
    import sqlalchemy
    import sqlamp
    import tests._testlib

    if options.quiet:
        verbosity = 0
    else:
        verbosity = 2
    unittest.TextTestRunner(verbosity=verbosity).run(get_suite())


def get_suite():
    suite = unittest.TestSuite()
    for test_module_name in TEST_MODULES:
        module = __import__(test_module_name, {}, {}, ['get_suite'])
        suite.addTest(module.get_suite())
    return suite


if __name__ == '__main__':
    main()

