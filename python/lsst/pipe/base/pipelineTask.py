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

"""This module defines PipelineTask class and related methods.
"""

__all__ = ["PipelineTask"]  # Classes in this module

from .task import Task


class PipelineTask(Task):
    """Base class for all pipeline tasks.

    This is an abstract base class for PipelineTasks which represents an
    algorithm executed by framework(s) on data which comes from data butler,
    resulting data is also stored in a data butler.

    PipelineTask inherits from a `pipe.base.Task` and uses the same
    configuration mechanism based on `pex.config`. PipelineTask sub-class
    typically implements `run()` method which receives Python-domain data
    objects and returns `pipe.base.Struct` object with resulting data.
    `run()` method is not supposed to perform any I/O, it operates entirely
    on in-memory objects. `runQuantum()` is the method (can be re-implemented
    in sub-class) where all necessary I/O is performed, it reads all input
    data from data butler into memory, calls `run()` method with that data,
    examines returned `Struct` object and saves some or all of that data back
    to data butler. `runQuantum()` method receives `daf.butler.Quantum`
    instance which defines all input and output datasets for a single
    invocation of PipelineTask.

    Subclasses must be constructable with exactly the arguments taken by the
    PipelineTask base class constructor, but may support other signatures as
    well.

    Attributes
    ----------
    canMultiprocess : bool, True by default (class attribute)
        This class attribute is checked by execution framework, sub-classes
        can set it to ``False`` in case task does not support multiprocessing.

    Parameters
    ----------
    config : `pex.config.Config`, optional
        Configuration for this task (an instance of ``self.ConfigClass``,
        which is a task-specific subclass of `PipelineTaskConfig`).
        If not specified then it defaults to `self.ConfigClass()`.
    log : `lsst.log.Log`, optional
        Logger instance whose name is used as a log name prefix, or ``None``
        for no prefix.
    initInputs : `dict`, optional
        A dictionary of objects needed to construct this PipelineTask, with
        keys matching the keys of the dictionary returned by
        `getInitInputDatasetTypes` and values equivalent to what would be
        obtained by calling `Butler.get` with those DatasetTypes and no data
        IDs.  While it is optional for the base class, subclasses are
        permitted to require this argument.
    """

    canMultiprocess = True

    def __init__(self, *, config=None, log=None, initInputs=None, **kwargs):
        super().__init__(config=config, log=log, **kwargs)

    @classmethod
    def getPerDatasetTypeDimensions(cls, config):
        """Return any Dimensions that are permitted to have different values
        for different DatasetTypes within the same quantum.

        Parameters
        ----------
        config : `Config`
            Configuration for this task.

        Returns
        -------
        dimensions : `~collections.abc.Set` of `Dimension` or `str`
            The dimensions or names thereof that should be considered
            per-DatasetType.

        Notes
        -----
        Any Dimension declared to be per-DatasetType by a PipelineTask must
        also be declared to be per-DatasetType by other PipelineTasks in the
        same Pipeline.

        The classic example of a per-DatasetType dimension is the
        ``CalibrationLabel`` dimension that maps to a validity range for
        master calibrations.  When running Instrument Signature Removal, one
        does not care that different dataset types like flat, bias, and dark
        have different validity ranges, as long as those validity ranges all
        overlap the relevant observation.
        """
        return frozenset()

    def run(self, **kwargs):
        """Run task algorithm on in-memory data.

        This method should be implemented in a subclass unless tasks overrides
        `adaptArgsAndRun` to do something different from its default
        implementation. With default implementation of `adaptArgsAndRun` this
        method will receive keyword arguments whose names will be the same as
        names of configuration fields describing input dataset types. Argument
        values will be data objects retrieved from data butler. If a dataset
        type is configured with ``scalar`` field set to ``True`` then argument
        value will be a single object, otherwise it will be a list of objects.

        If the task needs to know its input or output DataIds then it has to
        override `adaptArgsAndRun` method instead.

        Returns
        -------
        struct : `Struct`
            See description of `adaptArgsAndRun` method.

        Examples
        --------
        Typical implementation of this method may look like::

            def run(self, input, calib):
                # "input", "calib", and "output" are the names of the config fields

                # Assuming that input/calib datasets are `scalar` they are simple objects,
                # do something with inputs and calibs, produce output image.
                image = self.makeImage(input, calib)

                # If output dataset is `scalar` then return object, not list
                return Struct(output=image)

        """
        raise NotImplementedError("run() is not implemented")

    def runQuantum(self, butlerQC, quantumConnectionRefs):
        inputs = butlerQC.get(quantumConnectionRefs)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, quantumConnectionRefs)

    def getResourceConfig(self):
        """Return resource configuration for this task.

        Returns
        -------
        Object of type `~config.ResourceConfig` or ``None`` if resource
        configuration is not defined for this task.
        """
        return getattr(self.config, "resources", None)
