# Copyright 2022 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ruamel.yaml
import ruamel.yaml.scalarstring


def as_list(element):
    """
    If element is not a list, makes it a list whose only entry is element.
    If it's already a list, returns it as is

    :param element: The element to listify
    :type element: object
    :return: list
    """
    return element if isinstance(element, list) else [element]


def convert_value_to_yaml(value):
    def try_convert_to_bool(inner_value):
        if str(inner_value).lower() in ["true", "t", "y", "yes"]:
            return True
        if str(inner_value).lower() in ["false", "f", "n", "no"]:
            return False
        raise ValueError("Value is not boolean-convertable")

    def single_value_convert(inner_value):
        if inner_value is None:
            return inner_value

        inner_value = inner_value.strip()
        try:
            return int(inner_value)
        except ValueError:
            try:
                return float(inner_value)
            except ValueError:
                try:
                    return try_convert_to_bool(inner_value)
                except ValueError:
                    return ruamel.yaml.scalarstring.SingleQuotedScalarString(
                        inner_value
                    )

    if isinstance(value, list):
        for idx, k in enumerate(value):
            value[idx] = convert_value_to_yaml(k)
        return value

    if isinstance(value, dict):
        converted_value = {}
        for k, v in value.items():
            converted_value[convert_value_to_yaml(k)] = convert_value_to_yaml(v)
        return converted_value

    return single_value_convert(value)
