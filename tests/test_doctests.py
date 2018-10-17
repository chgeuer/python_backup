"""Test module for ScheduleParser"""

import doctest

from asebackupcli import scheduleparser

def load_tests(_loader, tests, _ignore):
    """Run doctests"""
    doctest.DocFileSuite()
    tests.addTests(doctest.DocTestSuite(scheduleparser))
    return tests
