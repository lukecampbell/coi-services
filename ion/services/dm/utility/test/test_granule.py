#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Tue Oct 16 09:14:37 EDT 2012
@file ion/services/dm/utility/test/test_granule.py
@brief Tests for granule
'''

from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.objects import ParameterContext, ParameterFunction, ParameterFunctionType as PFT

from gevent.event import Event
from nose.plugins.attrib import attr
from coverage_model import ParameterContext as CoverageParameterContext, QuantityType, AxisTypeEnum, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction

from ion.util.stored_values import StoredValueManager

import numpy as np

@attr('INT',group='dm')
class RecordDictionaryIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()

        self.rdt                      = None
        self.data_producer_id         = None
        self.provider_metadata_update = None
        self.event                    = Event()

    def verify_incoming(self, m,r,s):
        rdt = RecordDictionaryTool.load_from_granule(m)
        for k,v in rdt.iteritems():
            np.testing.assert_array_equal(v, self.rdt[k])
        self.assertEquals(m.data_producer_id, self.data_producer_id)
        self.assertEquals(m.provider_metadata_update, self.provider_metadata_update)
        self.assertNotEqual(m.creation_timestamp, None)
        self.event.set()


    def test_serialize_compatability(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('ctd extended', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        stream_id, route = self.pubsub_management.create_stream('ctd1', 'xp1', stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream, stream_id)

        sub_id = self.pubsub_management.create_subscription('sub1', stream_ids=[stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)
        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)

        verified = Event()
        def verifier(msg, route, stream_id):
            for k,v in msg.record_dictionary.iteritems():
                if v is not None:
                    self.assertIsInstance(v, np.ndarray)
            rdt = RecordDictionaryTool.load_from_granule(msg)
            for k,v in rdt.iteritems():
                self.assertIsInstance(rdt[k], np.ndarray)
                self.assertIsInstance(v, np.ndarray)
            verified.set()

        subscriber = StandaloneStreamSubscriber('sub1', callback=verifier)
        subscriber.start()
        self.addCleanup(subscriber.stop)

        publisher = StandaloneStreamPublisher(stream_id,route)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        ph.fill_rdt(rdt,10)
        publisher.publish(rdt.to_granule())
        self.assertTrue(verified.wait(10))


    def test_granule(self):
        
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('ctd', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':"GA03FLMA-RI001-13-CTDMOG999"})
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')
        self.addCleanup(self.pubsub_management.delete_stream_definition,stream_def_id)

        stream_id, route = self.pubsub_management.create_stream('ctd_stream', 'xp1', stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream,stream_id)
        publisher = StandaloneStreamPublisher(stream_id, route)

        subscriber = StandaloneStreamSubscriber('sub', self.verify_incoming)
        subscriber.start()
        self.addCleanup(subscriber.stop)

        subscription_id = self.pubsub_management.create_subscription('sub', stream_ids=[stream_id])
        self.pubsub_management.activate_subscription(subscription_id)


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp'] = np.random.randn(10) * 10 + 30
        rdt['pressure'] = [20] * 10

        self.assertEquals(set(pdict.keys()), set(rdt.fields))
        self.assertEquals(pdict.temporal_parameter_name, rdt.temporal_parameter)

        self.assertEquals(rdt._stream_config['reference_designator'],"GA03FLMA-RI001-13-CTDMOG999")

        self.rdt = rdt
        self.data_producer_id = 'data_producer'
        self.provider_metadata_update = {1:1}

        publisher.publish(rdt.to_granule(data_producer_id='data_producer', provider_metadata_update={1:1}))

        self.assertTrue(self.event.wait(10))
        
        self.pubsub_management.deactivate_subscription(subscription_id)
        self.pubsub_management.delete_subscription(subscription_id)
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.array([None,None,None])
        self.assertTrue(rdt['time'] is None)
        
        rdt['time'] = np.array([None, 1, 2])
        self.assertEquals(rdt['time'][0], rdt.fill_value('time'))


        stream_def_obj = self.pubsub_management.read_stream_definition(stream_def_id)
        rdt = RecordDictionaryTool(stream_definition=stream_def_obj)
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)


        granule = rdt.to_granule()
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['time'], np.arange(20))
        np.testing.assert_array_equal(rdt['temp'], np.arange(20))

        
    def test_filter(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        filtered_stream_def_id = self.pubsub_management.create_stream_definition('filtered', parameter_dictionary_id=pdict_id, available_fields=['time', 'temp'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, filtered_stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=filtered_stream_def_id)
        self.assertEquals(rdt._available_fields,['time','temp'])
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)
        with self.assertRaises(KeyError):
            rdt['pressure'] = np.arange(20)

        granule = rdt.to_granule()
        rdt2 = RecordDictionaryTool.load_from_granule(granule)
        self.assertEquals(rdt._available_fields, rdt2._available_fields)
        self.assertEquals(rdt.fields, rdt2.fields)
        for k,v in rdt.iteritems():
            self.assertTrue(np.array_equal(rdt[k], rdt2[k]))
        


    def test_rdt_param_funcs(self):
        param_funcs = {
            'identity' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.interpolation',
                'function' : 'identity',
                'args':['x']
            },
            'ctd_tempwat' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.ctd_functions',
                'function' : 'ctd_sbe37im_tempwat',
                'args' : ['t0']
            },
            'ctd_preswat' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.ctd_functions',
                'function' : 'ctd_sbe37im_preswat',
                'args' : ["p0", "p_range_psia"]
            },
            'ctd_condwat' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.ctd_functions',
                'function' : 'ctd_sbe37im_condwat',
                'args' : ['c0']
            },
            'ctd_pracsal' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.ctd_functions',
                'function' : 'ctd_pracsal',
                'args' : ['c', 't', 'p']
            },
            'ctd_density' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.ctd_functions',
                'function' : 'ctd_density',
                'args' : ['SP','t','p','lat','lon']
            }
        }

        pfunc_ids = {}
        for name, param_def in param_funcs.iteritems():
            paramfunc = ParameterFunction(name, **param_def)
            pf_id = self.dataset_management.create_parameter_function(paramfunc)
            pfunc_ids[name] = pf_id


        params = {
            'time' : {
                'parameter_type' : 'quantity',
                'value_encoding' : 'float64',
                'units' : 'seconds since 1900-01-01'
            },
            'temperature_counts' : {
                'parameter_type' : 'quantity',
                'value_encoding' : 'float32',
                'units' : '1'
            },
            'pressure_counts' : {
                'parameter_type' : 'quantity',
                'value_encoding' : 'float32',
                'units' : '1'
            },
            'conductivity_counts' : {
                'parameter_type' : 'quantity',
                'value_encoding' : 'float32',
                'units' : '1'
            },
            'temperature' : {
                'parameter_type' : 'function',
                'parameter_function_id' : pfunc_ids['ctd_tempwat'],
                'parameter_function_map' : { 't0' : 'temperature_counts'},
                'value_encoding' : 'float32',
                'units' : 'deg_C'
            },
            'pressure' : {
                'parameter_type' : 'function',
                'parameter_function_id' : pfunc_ids['ctd_preswat'],
                'parameter_function_map' : {'p0' : 'pressure_counts', 'p_range_psia' : 679.34040721},
                'value_encoding' : 'float32',
                'units' : 'dbar'
            },
            'conductivity' : {
                'parameter_type' : 'function',
                'parameter_function_id' : pfunc_ids['ctd_condwat'],
                'parameter_function_map' : {'c0' : 'conductivity_counts'},
                'value_encoding' : 'float32',
                'units' : 'Sm-1'
            },
            'salinity' : {
                'parameter_type' : 'function',
                'parameter_function_id' : pfunc_ids['ctd_pracsal'],
                'parameter_function_map' : {'c' : 'conductivity', 't' : 'temperature', 'p' : 'pressure'},
                'value_encoding' : 'float32',
                'units' : '1'
            },
            'density' : {
                'parameter_type' : 'function',
                'parameter_function_id' : pfunc_ids['ctd_density'],
                'parameter_function_map' : {
                    'SP' : 'salinity',
                    't' : 'temperature',
                    'p' : 'pressure',
                    'lat' : 'lat',
                    'lon' : 'lon'
                },
                'value_encoding' : 'float32',
                'units' : 'kg m-1'
            },
            'lat' : {
                'parameter_type' : 'sparse',
                'value_encoding' : 'float32',
                'units' : 'degrees_north'
            },
            'lon' : {
                'parameter_type' : 'sparse',
                'value_encoding' : 'float32',
                'units' : 'degrees_east'
            }
        }
        param_dict = {}
        for name, param in params.iteritems():
            pcontext = ParameterContext(name, **param)
            param_id = self.dataset_management.create_parameter(pcontext)
            param_dict[name] = param_id
            
        pdict_id = self.dataset_management.create_parameter_dictionary('ctd_test', param_dict.values(), 'time')
        stream_def_id = self.pubsub_management.create_stream_definition('ctd_test', parameter_dictionary_id=pdict_id)


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temperature_counts'] = [280000]
        rdt['conductivity_counts'] = [100000]
        rdt['pressure_counts'] = [2789]

        rdt['lat'] = [45]
        rdt['lon'] = [-71]

        np.testing.assert_allclose(rdt['density'], np.array([1001.00543606]))

    def test_rdt_lookup(self):
        rdt = self.create_lookup_rdt()

        self.assertTrue('offset_a' in rdt.lookup_values())
        self.assertFalse('offset_b' in rdt.lookup_values())

        rdt['time'] = [0]
        rdt['temp'] = [10.0]
        rdt['offset_a'] = [2.0]
        self.assertEquals(rdt['offset_b'], None)
        self.assertEquals(rdt.lookup_values(), ['offset_a'])
        np.testing.assert_array_almost_equal(rdt['calibrated'], np.array([12.0]))

        svm = StoredValueManager(self.container)
        svm.stored_value_cas('coefficient_document', {'offset_b':2.0})
        svm.stored_value_cas("GA03FLMA-RI001-13-CTDMOG999_OFFSETC", {'offset_c':3.0})
        rdt.fetch_lookup_values()
        np.testing.assert_array_equal(rdt['offset_b'], np.array([2.0]))
        np.testing.assert_array_equal(rdt['calibrated_b'], np.array([14.0]))
        np.testing.assert_array_equal(rdt['offset_c'], np.array([3.0]))


    def create_rdt(self):
        contexts, pfuncs = self.create_pfuncs()
        context_ids = list(contexts.itervalues())

        pdict_id = self.dataset_management.create_parameter_dictionary(name='functional_pdict', parameter_context_ids=context_ids, temporal_context='test_TIME')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)
        stream_def_id = self.pubsub_management.create_stream_definition('functional', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt

    def create_lookup_rdt(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_lookups()

        stream_def_id = self.pubsub_management.create_stream_definition('lookup', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':"GA03FLMA-RI001-13-CTDMOG999"})
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt


    def create_pfuncs(self):
        
        contexts = {}
        funcs = {}

        t_ctxt = ParameterContext(name='TIME', 
                                  parameter_type='quantity',
                                  value_encoding='float64',
                                  units='seconds since 1900-01-01')
        t_ctxt_id = self.dataset_management.create_parameter(t_ctxt)
        contexts['TIME'] = t_ctxt_id

        lat_ctxt = ParameterContext(name='LAT', 
                                    parameter_type="sparse",
                                    value_encoding='float32',
                                    units='degrees_north')
        lat_ctxt_id = self.dataset_management.create_parameter(lat_ctxt)
        contexts['LAT'] = lat_ctxt_id

        lon_ctxt = ParameterContext(name='LON', 
                                    parameter_type='sparse',
                                    value_encoding='float32',
                                    units='degrees_east')
        lon_ctxt_id = self.dataset_management.create_parameter(lon_ctxt)
        contexts['LON'] = lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext(name='TEMPWAT_L0', 
                parameter_type='quantity',
                value_encoding='float32',
                units='deg_C')
        temp_ctxt_id = self.dataset_management.create_parameter(temp_ctxt)
        contexts['TEMPWAT_L0'] = temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext(name='CONDWAT_L0', 
                parameter_type='quantity',
                value_encoding='float32',
                units='S m-1')
        cond_ctxt_id = self.dataset_management.create_parameter(cond_ctxt)
        contexts['CONDWAT_L0'] = cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext(name='PRESWAT_L0', 
                parameter_type='quantity',
                value_encoding='float32',
                units='dbar')
        press_ctxt_id = self.dataset_management.create_parameter(press_ctxt)
        contexts['PRESWAT_L0'] = press_ctxt_id


        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(T / 10000) - 10'
        expr = ParameterFunction(name='TEMPWAT_L1',
                function_type=PFT.NUMEXPR,
                function=tl1_func,
                args=['T'])
        expr_id = self.dataset_management.create_parameter_function(expr)
        funcs['TEMPWAT_L1'] = expr_id

        tl1_pmap = {'T': 'TEMPWAT_L0'}
        tempL1_ctxt = ParameterContext(name='TEMPWAT_L1', 
                parameter_type='function',
                parameter_function_id=expr_id,
                parameter_function_map=tl1_pmap,
                value_encoding='float32',
                units='deg_C')
        tempL1_ctxt_id = self.dataset_management.create_parameter(tempL1_ctxt)
        contexts['TEMPWAT_L1'] = tempL1_ctxt_id

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(C / 100000) - 0.5'
        expr = ParameterFunction(name='CONDWAT_L1',
                function_type=PFT.NUMEXPR,
                function=cl1_func,
                args=['C'])
        expr_id = self.dataset_management.create_parameter_function(expr)
        funcs['CONDWAT_L1'] = expr_id

        cl1_pmap = {'C': 'CONDWAT_L0'}
        condL1_ctxt = ParameterContext(name='CONDWAT_L1', 
                parameter_type='function',
                parameter_function_id=expr_id,
                parameter_function_map=cl1_pmap,
                value_encoding='float32',
                units='S m-1')
        condL1_ctxt_id = self.dataset_management.create_parameter(condL1_ctxt)
        contexts['CONDWAT_L1'] = condL1_ctxt_id

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(P * p_range / (0.85 * 65536)) - (0.05 * p_range)'
        expr = ParameterFunction(name='PRESWAT_L1',function=pl1_func,function_type=PFT.NUMEXPR,args=['P','p_range'])
        expr_id = self.dataset_management.create_parameter_function(expr)
        funcs['PRESWAT_L1'] = expr_id
        
        pl1_pmap = {'P': 'PRESWAT_L0', 'p_range': 679.34040721}
        presL1_ctxt = ParameterContext(name='PRESWAT_L1',
                parameter_type='function',
                parameter_function_id=expr_id,
                parameter_function_map=pl1_pmap,
                value_encoding='float32',
                units='S m-1')
        presL1_ctxt_id = self.dataset_management.create_parameter(presL1_ctxt)
        contexts['PRESWAT_L1'] = presL1_ctxt_id

        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = ['C', 't', 'p']
        expr = ParameterFunction(name='PRACSAL',function_type=PFT.PYTHON,function=sal_func,owner=owner,args=sal_arglist)
        expr_id = self.dataset_management.create_parameter_function(expr)
        funcs['PRACSAL'] = expr_id
        
        c10_f = ParameterFunction(name='c10', function_type=PFT.NUMEXPR, function='C*10', args=['C'])
        expr_id = self.dataset_management.create_parameter_function(c10_f)
        c10 = ParameterContext(name='c10', 
                parameter_type='function',
                parameter_function_id=expr_id,
                parameter_function_map={'C':'CONDWAT_L1'},
                value_encoding='float32',
                units='1')
        c10_id = self.dataset_management.create_parameter(c10)
        contexts['c10'] = c10_id

        # A magic function that may or may not exist actually forms the line below at runtime.
        sal_pmap = {'C': 'c10', 't': 'TEMPWAT_L1', 'p': 'PRESWAT_L1'}
        sal_ctxt = ParameterContext(name='PRACSAL', 
                parameter_type='function',
                parameter_function_id=expr_id,
                parameter_function_map=sal_pmap,
                value_encoding='float32',
                units='g kg-1')

        sal_ctxt_id = self.dataset_management.create_parameter(sal_ctxt)
        contexts['PRACSAL'] = sal_ctxt_id

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'gsw'
        abs_sal_expr = PythonFunction('abs_sal', owner, 'SA_from_SP', ['PRACSAL', 'PRESWAT_L1', 'LON','LAT'])
        cons_temp_expr = PythonFunction('cons_temp', owner, 'CT_from_t', [abs_sal_expr, 'TEMPWAT_L1', 'PRESWAT_L1'])
        dens_expr = PythonFunction('DENSITY', owner, 'rho', [abs_sal_expr, cons_temp_expr, 'PRESWAT_L1'])
        dens_ctxt = CoverageParameterContext('DENSITY', param_type=ParameterFunctionType(dens_expr), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'
        dens_ctxt_id = self.dataset_management.create_parameter_context(name='DENSITY', parameter_context=dens_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, dens_ctxt_id)
        contexts['DENSITY'] = dens_ctxt_id
        return contexts, funcs

    


        
