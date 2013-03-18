#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.test.test_oms_simulator
@file    ion/agents/platform/rsn/simulator/test/test_oms_simulator.py
@author  Carlos Rueda
@brief   Test cases for the simulator, by default in embedded form,
         but the OMS environment variable can be used to indicate other.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.unit_test import IonUnitTestCase

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr


@attr('UNIT', group='sa')
class Test(IonUnitTestCase, OmsTestMixin):
    """
    Test cases for the simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()
        OmsTestMixin.start_http_server()
        cls.oms = CIOMSClientFactory.create_instance()

    @classmethod
    def tearDownClass(cls):
        if isinstance(cls.oms, CIOMSSimulator):
            cls.oms._deactivate_simulator()
        event_notifications = OmsTestMixin.stop_http_server()
        log.info("event_notifications = %s" % str(event_notifications))