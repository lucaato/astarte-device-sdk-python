# This file is part of Astarte.
#
# Copyright 2023 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from re import sub, match

from astarte.device.mapping import Mapping
from astarte.device.exceptions import ValidationError

DEVICE = "device"
SERVER = "server"


class Interface:
    """
    Class that represent an Interface definition

    Interfaces are a core concept of Astarte which defines how data is exchanged between Astarte
    and its peers. They are not to be intended as OOP interfaces, but rather as the following
    definition:

    In Astarte each interface has an owner, can represent either a continuous data stream or a
    snapshot of a set of properties, and can be either aggregated into an object or be an
    independent set of individual members.

    Attributes
    ----------
        name: str
            Interface name
        version_major: int
            Interface version major number
        version_minor: int
            Interface version minor number
        type: str
            Interface type
        ownership: str
            Interface ownership
        aggregation: str
            Interface aggregation policy
        mappings: dict(Mapping)
            Interface mapping dictionary, keys are the endpoint of each mapping
    """

    def __init__(self, interface_definition: dict):
        """
        Parameters
        ----------
        interface_definition: dict
            An Astarte Interface definition in the form of a Python dictionary. Usually obtained
            by using json.loads on an Interface file.

        Raises
        ------
        ValueError
            if both version_major and version_minor numbers are set to 0
        """
        self.name: str = interface_definition["interface_name"]
        self.version_major: int = interface_definition["version_major"]
        self.version_minor: int = interface_definition["version_minor"]

        if not self.version_major and not self.version_minor:
            raise ValueError(f"Both Major and Minor versions set to 0 for interface {self.name}")

        self.type: str = interface_definition["type"]
        self.ownership = interface_definition.get("ownership", DEVICE)
        self.aggregation = interface_definition.get("aggregation", "")
        self.mappings = {}
        for mapping_definition in interface_definition["mappings"]:
            mapping = Mapping(mapping_definition, self.type)
            self.mappings[mapping.endpoint] = mapping

    def is_aggregation_object(self) -> bool:
        """
        Check if the current Interface is a datastream with aggregation object
        Returns
        -------
        bool
            True if aggregation: object
        """
        return self.aggregation == "object"

    def is_server_owned(self) -> bool:
        """
        Check the Interface ownership
        Returns
        -------
        bool
            True if ownership: server
        """
        return self.ownership == "server"

    def is_type_properties(self):
        """
        Check the Interface type
        Returns
        -------
        bool
            True if type: properties
        """
        return self.type == "properties"

    def get_mapping(self, endpoint) -> Mapping | None:
        """
        Retrieve the Mapping with the given endpoint from the Interface
        Parameters
        ----------
        endpoint: str
            The Mapping endpoint

        Returns
        -------
        Mapping or None
            The Mapping if found, None otherwise
        """
        for path, mapping in self.mappings.items():
            regex = sub(r"%{\w+}", r"[^/+#]+", path)
            if match(regex + "$", endpoint):
                return mapping

        return None

    def validate(self, path: str, payload, timestamp: datetime) -> ValidationError | None:
        """
        Interface Data validation

        Parameters
        ----------
        path: str
            Data endpoint in interface
        payload: object
            Data to validate
        timestamp: datetime or None
            Timestamp associated to the payload

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        # Check the interface has device ownership
        if self.ownership != DEVICE:
            return ValidationError(f"The interface {self.name} is not owned by the device.")
        if not self.is_aggregation_object():
            # Check the validity of the path
            mapping = self.get_mapping(path)
            if mapping:
                return mapping.validate(payload, timestamp)

            return ValidationError(f"Path {path} not in the {self.name} interface.")

        if not isinstance(payload, dict):
            return ValidationError(
                f"The interface {self.name} is aggregate, but the payload is not a dictionary."
            )

        # Validate all paths
        for k, v in payload.items():
            mapping = self.get_mapping(f"{path}/{k}")
            if mapping:
                result = mapping.validate(v, timestamp)
                if result:
                    return result
            else:
                return ValidationError(f"Path {path}/{k} not in the {self.name} interface.")

        # Check all elements are present
        for endpoint in self.mappings:
            non_common_endpoint = "/".join(endpoint.split("/")[len(path.split("/")) :])
            if non_common_endpoint not in payload:
                return ValidationError(
                    f"Path {endpoint} of {self.name} interface is not in the payload."
                )

        return None
