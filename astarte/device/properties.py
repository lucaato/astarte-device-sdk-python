# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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
"""
Defines a class that can be used externally from the library to easily retrieve properties.
"""
from typing import Optional

from astarte.device.database import AstarteDatabase, PropertyData
from astarte.device.device import TypeAstarteData
from astarte.device.interface import Interface
from astarte.device.introspection import Introspection


class PropertyAccess:
    """
    Allows retrieval of stored properties, returned by the Device.access_props method
    """

    database: AstarteDatabase
    interfaces: Introspection

    def __init__(self, database: AstarteDatabase):
        if not database:
            raise ValueError("Property access must not be initialized with a None database")

        self.database = database

    def get_property(self, interface: str, path: str) -> Optional[TypeAstarteData]:
        interface: Optional[Interface] = self.interfaces.get_interface(interface)
        if interface == None:
            raise ValueError("The passed interface name is not stored in the device")
        if interface.get_mapping(path) == None:
            raise ValueError("The passed path does not match any of the interface endpoints")

        self.database.load_prop(interface.name, interface.version_major, path)

    def get_interface_props(self, interface: str) -> list[PropertyData]:
        interface: Optional[Interface] = self.interfaces.get_interface(interface)
        if interface == None:
            raise ValueError("The passed interface name is not stored in the device")

        return self.database.load_interface_props(interface.name)

    def get_all_props(self) -> list[PropertyData]:
        return self.database.load_all_props()

    def get_device_props(self) -> list[PropertyData]:
        return self.database.load_device_props()

    def get_server_props(self) -> list[PropertyData]:
        return self.database.load_server_props()
