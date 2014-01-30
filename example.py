from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from coverage_model import ParameterContext, ParameterFunctionType
from coverage_model.parameter_functions import AbstractFunction
import pkg_resources
import importlib
class ExampleDataProcess:

    def __init__(self, container, rdt):
        self.container = container
        self.dataset_management = DatasetManagementServiceClient()
        self.added_functions = []
        self.rdt = rdt

    def execute(self):
        for name in self.added_functions:
            self.rdt[name] = self.rdt[name]

    def add_function(self, name, parameter_function_id, pmap):
        function = self.load_parameter_function(parameter_function_id)
        function.param_map = pmap
        ctx = ParameterContext(name, param_type=ParameterFunctionType(function))
        self.rdt._pdict.add_context(ctx)
        self.rdt._rd[name] = None
        self.added_functions.append(name)

    def load_parameter_function(self, parameter_function_id):
        parameter_function = self.dataset_management.read_parameter_function(parameter_function_id)
        pfunc = AbstractFunction.load(parameter_function.parameter_function)
        return pfunc

    def load_egg(self, eggname, import_string):
        try:
            importlib.import_module(import_string)
        except ImportError:
            pkg_resources.working_set.add_entry(eggname)
            importlib.import_module(import_string)

    

