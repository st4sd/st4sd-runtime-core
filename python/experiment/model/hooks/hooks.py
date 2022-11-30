# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2022
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

'''
Definitions of hooks. These hooks below are expected to exist (under certain conditions) inside the `interface.py` file
of the `hooks` folder of a virtual experiment definition.

When an Engine fails the runtime system may attempt to load the Restart hook

    import experiment.model.codes
    def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode) -> str:
        """Inspects component to determine how the runtime system should handle restarting this component

        Returns:
            A string out of experiment.model.codes.restartContexts that explains how the runtime system should handle
                restarting this component
        """


When interface.inputSpec.inputExtractionMethod.hookGetInputIds (FlowIR) is set.

    def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
        """This hook discovers the system ids in an input file

        Args:
            input_id_file: A path to the file containing the input ids.
                The value of the field interface.inputSpec.source.file
            variables: The global variables of the virtual experiment instance.
                This Argument is Optional, the hook does *not* need to implement it.

        Returns:
            Returns a list of the ids of the inputs in the naming specification defined by interface

        Raises:
            IOError: if the file is not present or cannot be read.
        """

When interface.inputSpec.hasAdditionalData (FlowIR) is set to True

    def get_additional_input_data(input_id: str, instance_dir: str) -> List[str]:
        """This hook discovers additional data that are associated with an input id.

        Args:
            input_id: The id of an input
            instance_dir: The absolute path to the instance directory

        Returns:
            Returns a list of absolute paths that are associated input_id. The paths must exist.
        """

When the x'th property has interface.propertiesSpec[x].propertyExtractionMethod.hookGetProperties (FlowIR)

    import experiment.model.errors
    import pandas
    def get_properties(property_name:str, property_output_file: str, input_id_file: str, variables: Dict[str, str]
    ) -> pandas.DataFrame:
        """This hook discovers the values of a property for all inputs ids for which it is measured

        Args:
            property_output_file: A file path. The value full path of the value of the `data-in` key of the `output`
                element `interface.propertiesSpec.name.source.output` references.
            property_name: The value of interface.propertiesSpec.name.
            input_id_file: A path to the file containing the input ids.
                The value of the field interface.inputSpec.source.file
            variables: The global variables of the virtual experiment instance.
                This Argument is Optional, the hook does *not* need to implement it.

        Returns:
            Returns a DataFrame with at least two columns (input-id, $PropertyName). Each row correspond to an input.
                The "input-id" column contains the ids, as returned by get_input_ids, for which
                the property was measured.

        Raises:
            experiment.model.errors.CannotReadPropertyOutputFileError: There is no property file with the given name,
                or it cannot be accessed
            experiment.model.errors.CannotReadInputIdFileError: There is no input file with the given name,
                or it cannot be accessed
            experiment.model.errors.UnknownPropertyError: The property name passed is not known to the hook
            experiment.model.errors.PropertyNotMeasuredError: The property output files does not have a property
                with this name
        """
'''

