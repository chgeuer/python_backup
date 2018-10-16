import unittest
import doctest

from asebackupcli import azurevminstancemetadata
from asebackupcli import backupconfiguration
from asebackupcli import backupconfigurationfile
from asebackupcli import businesshours
from asebackupcli import naming
from asebackupcli import scheduleparser
from asebackupcli import timing
from asebackupcli import backupagent

def load_tests(loader, tests, ignore):
    # tests.addTests(doctest.DocTestSuite(azurevminstancemetadata))
    # tests.addTests(doctest.DocTestSuite(backupconfiguration))
    # tests.addTests(doctest.DocTestSuite(backupconfigurationfile))
    # tests.addTests(doctest.DocTestSuite(businesshours))
    # tests.addTests(doctest.DocTestSuite(naming))
    # tests.addTests(doctest.DocTestSuite(scheduleparser))
    # tests.addTests(doctest.DocTestSuite(timing))
    # tests.addTests(doctest.DocTestSuite(backupagent))
    return tests
