# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining config classes for PipelineTask.
"""

__all__ = ["ResourceConfig", "PipelineTaskConfig",
           "PipelineTaskConnections"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from collections import UserDict
import dataclasses
import itertools
import string
import typing

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig

from .struct import Struct

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class ScalarError(TypeError):
    """Exception raised when dataset type is configured as scalar
    but there are multiple DataIds in a Quantum for that dataset.

    Parameters
    ----------
    key : `str`
        Name of the configuration field for dataset type.
    numDataIds : `int`
        Actual number of DataIds in a Quantum for this dataset type.
    """
    def __init__(self, key, numDataIds):
        super().__init__(("Expected scalar for output dataset field {}, "
                          "received {} DataIds").format(key, numDataIds))


@dataclasses.dataclass(frozen=True)
class BaseConnection:
    name: str
    storageClass: str
    differLoad: bool = False
    multiple: bool = False
    checkFunction: typing.Callable = None

    def __get__(self, inst, klass):
        if inst is None:
            return self
        if not hasattr(self, '_objCache'):
            object.__setattr__(self, '_objCache', {})
        params = {}
        for field in dataclasses.fields(self):
            params[field.name] = getattr(self, field.name)
        params['name'] = inst._nameOverrides[self.varName]
        return self._objCache.setdefault(id(inst), self.__class__(**params))


@dataclasses.dataclass(frozen=True)
class DimensionedConnection(BaseConnection):
    dimensions: typing.Iterable[str] = ()


@dataclasses.dataclass(frozen=True)
class Input(DimensionedConnection):
    pass


@dataclasses.dataclass(frozen=True)
class AuxiliaryInput(DimensionedConnection):
    pass


@dataclasses.dataclass(frozen=True)
class Output(DimensionedConnection):
    pass


@dataclasses.dataclass(frozen=True)
class InitInput(BaseConnection):
    pass


@dataclasses.dataclass(frozen=True)
class InitOutput(BaseConnection):
    pass


class PipelineTaskConnectionDict(UserDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data['inputs'] = []
        self.data['auxiliaryInputs'] = []
        self.data['outputs'] = []
        self.data['initInputs'] = []
        self.data['initOutputs'] = []
        self.data['allConnections'] = {}

        self.data['Input'] = Input
        self.data['AuxiliaryInput'] = AuxiliaryInput
        self.data['Output'] = Output
        self.data['InitInput'] = InitInput
        self.data['InitOutput'] = InitOutput

    def __setitem__(self, name, value):
        if isinstance(value, Input):
            self.data['inputs'].append(name)
        if isinstance(value, AuxiliaryInput):
            self.data['auxiliaryInputs'].append(name)
        if isinstance(value, Output):
            self.data['outputs'].append(name)
        if isinstance(value, InitInput):
            self.data['initInputs'].append(name)
        if isinstance(value, InitOutput):
            self.data['initOutputs'].append(name)
        if isinstance(value, BaseConnection):
            object.__setattr__(value, 'varName', name)
            self.data['allConnections'][name] = value
        super().__setitem__(name, value)


class PipelineTaskConnectionsMetaclass(type):
    def __prepare__(name, args):  # noqa: 805
        return PipelineTaskConnectionDict()

    def __new__(cls, name, bases, dct):
        dimensionsValueError = TypeError("PipelineTaskConnections class must be created with a dimensions "
                                         "attribute which is an iterable of dimension names")

        if name != 'PipelineTaskConnections':
            if 'dimensions' not in dct:
                raise dimensionsValueError
            try:
                dct['dimensions'] = set(dct['dimensions'])
            except TypeError:
                raise dimensionsValueError
            allTemplates = set()
            stringFormatter = string.Formatter()
            for name, obj in dct['allConnections'].items():
                nameValue = obj.name
                for param in stringFormatter.parse(nameValue):
                    allTemplates.add(param)
            if len(allTemplates) > 0 and 'defaultTemplates' not in dct:
                raise TypeError("PipelineTaskConnection class contains templated attribute names, but no "
                                "defaut templates were proveded, add a dictionary attribute named "
                                "defaultTemplates which contains the mapping between template key and value")
            if len(allTemplates) < 0:
                defaultTemplateKeys = set(dct['defaultTemplates'].keys())
                templateDifference = allTemplates.difference(defaultTemplateKeys)
                if templateDifference:
                    raise TypeError(f"Default template keys were not provided for {templateDifference}")
                nameTemplateIntersection = allTemplates.inserset(set(dct['allConnections'].keys()))
                if len(nameTemplateIntersection) > 0:
                    raise TypeError(f"Template parameters cannot share names with Class attributes")
        return super().__new__(cls, name, bases, dict(dct))


class PipelineTaskConnections(metaclass=PipelineTaskConnectionsMetaclass):
    def __init__(self, *, config=None):
        if config is None or not isinstance(config, PipelineTaskConfig):
            raise ValueError("PipelineTaskConnections must be instantiated with"
                             " a PipelineTaskConfig instance")
        self.config = config
        templateValues = {name: getattr(config.connections, name) for name in getattr(self,
                          'defaultTemplates', {}).keys()}
        self._nameOverrides = {name: getattr(config.connections, name).format(**templateValues)
                               for name in self.allConnections.keys()}

    def buildDatasetRefs(self, quantum, butler):
        inputDatasetRefs = {}
        inputDataIds = {}
        outputDatasetRefs = {}
        outputDataIds = {}
        for (refs, ids), names in zip(((inputDatasetRefs, inputDataIds), (outputDatasetRefs, outputDataIds)),
                                      (itertools.chain(self.inputs, self.auxiliaryInputs), self.outputs)):
            for attributeName in names:
                attribute = getattr(self, attributeName)
                quantumInputRefs = quantum.predictedInputs[attribute.name]
                quantumInputIds = [dataRef.dataId for dataRef in quantumInputRefs]
                if not attribute.multi:
                    if len(quantumInputRefs) != 1:
                        raise ScalarError(attributeName, len(quantumInputRefs))
                    quantumInputRefs = quantumInputRefs[0]
                    quantumInputIds = quantumInputIds[0]
                refs[attributeName] = quantumInputRefs
                ids[attributeName] = quantumInputIds
        return Struct(inputs=inputDatasetRefs, outputs=outputDatasetRefs),\
            Struct(inputs=inputDataIds, outputs=outputDataIds)


class PipelineTaskConfigMeta(pexConfig.ConfigMeta):
    def __new__(cls, name, bases, dct, **kwargs):
        if name != "NewPipelineTaskConfig":
            if 'pipelineConnections' not in kwargs:
                raise NameError("PipelineTaskConfig must be defined with connections class")
            connectionsClass = kwargs['pipelineConnections']
            if not issubclass(connectionsClass, PipelineTaskConnections):
                raise ValueError("Can only assign a PipelineTaskConnectionClass to pipelineConnections")
            configConnectionsNamespace = {}
            for fieldName, obj in connectionsClass.allConnections.items():
                configConnectionsNamespace[fieldName] = pexConfig.Field(dtype=str,
                                                                        doc=f"name for "
                                                                            "connection {fieldName}",
                                                                        default=obj.name)
            if hasattr(connectionsClass, 'defaultTemplates'):
                docString = "Template parameter used to format corresponding field template parameter"
                for templateName, default in connectionsClass.defaultTemplates.items():
                    configConnectionsNamespace[templateName] = pexConfig.Field(dtype=str,
                                                                               doc=docString,
                                                                               default=default)
            configConnectionsNamespace['connections'] = connectionsClass

            Connections = type("Connections", (pexConfig.Config,), configConnectionsNamespace)
            dct['connections'] = pexConfig.ConfigField(dtype=Connections,
                                                       doc='Configurations describing the'
                                                           'connections of the PipelineTask to datatypes')
            dct['ConnectionsConfigClass'] = Connections
        inst = super().__new__(cls, name, bases, dct)
        return inst

    def __init__(self, name, bases, dct, **kwargs):
        super().__init__(name, bases, dct)


class PipelineTaskConfig(pexConfig.Config, metaclass=PipelineTaskConfigMeta):
    pass

# class FakePipelineConnections(PipelineTaskConnections):
#     exposure = Input(name='{foo}_deep', storageClass="Exposure")
#     calexp = Output(name='deep', storageClass="Exposure")
#     dimensions = ()
#     defaultTemplates = {'foo': 'coadd'}
#
#
# class FakePipelineConfig(NewPipelineTaskConfig, pipelineConnections=FakePipelineConnections):
#     pass


class ResourceConfig(pexConfig.Config):
    """Configuration for resource requirements.

    This configuration class will be used by some activators to estimate
    resource use by pipeline. Additionally some tasks could use it to adjust
    their resource use (e.g. reduce the number of threads).

    For some resources their limit can be estimated by corresponding task,
    in that case task could set the field value. For many fields defined in
    this class their associated resource used by a task will depend on the
    size of the data and is not known in advance. For these resources their
    value will be configured through overrides based on some external
    estimates.
    """
    minMemoryMB = pexConfig.Field(dtype=int, default=None, optional=True,
                                  doc="Minimal memory needed by task, can be None if estimate is unknown.")
    minNumCores = pexConfig.Field(dtype=int, default=1,
                                  doc="Minimal number of cores needed by task.")
