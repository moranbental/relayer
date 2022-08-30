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
import os
import shutil
from ruamel import yaml

import relayer.core
import relayer.clients.logging

from twisted.trial import unittest


class RelayerTestCase(unittest.TestCase):
    _logger = relayer.clients.logging.Client(
        "relayer",
        log_colors="always",
        initial_severity=relayer.clients.logging.Severity.Verbose,
    ).logger

    def setUp(self):
        def get_path_to_file(file_name):
            return os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "../fixtures/{0}".format(file_name),
                )
            )

        # we create a modifiable file out of the original one, to test on it and in the end remove it
        original_file_name = "relayer_test.yml"
        modifiable_file_name = "relayer_test_copy.yml"
        original_file_path = get_path_to_file(original_file_name)

        # auxiliary file (for --from-file updates)
        self._aux_file_path = get_path_to_file("relayer_aux_test.yml")

        # make a modifiable copy of the yml fixture
        self._modifiable_file_path = get_path_to_file(modifiable_file_name)
        shutil.copyfile(original_file_path, self._modifiable_file_path)

        self._original_config_dict = self._relayer_config_yml_to_dict(
            self._modifiable_file_path
        )
        self._aux_config_dict = self._relayer_config_yml_to_dict(self._aux_file_path)

        self._relayer = relayer.core.Relayer(
            self._logger, self._modifiable_file_path
        )

    def tearDown(self):

        # remove the modifiable copy of the yml file
        os.remove(self._modifiable_file_path)

    def test_nothing_changed(self):
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)
        self.assertEquals(result, self._original_config_dict)

    def test_addition_to_empty_file(self):

        # clear the file
        open(self._modifiable_file_path, "w").close()

        self._relayer.relayer_config(
            add_kvs=["aa=bb"],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)
        self.assertEquals(result, {"aa": "bb"})

    def test_addition_to_existing_file(self):

        # add a totally new field
        self._relayer.relayer_config(
            add_kvs=["field_2.sub_field_0=123"],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(result["field_2"]["sub_field_0"], 123)

        # add a sub-field to existing field and replace other field
        self._relayer.relayer_config(
            add_kvs=[
                "field_0.sub_field_0.sub_sub_field_test=456",
                "field_2.sub_field_1=a",
            ],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        # the previous field we added is still there
        self.assertEquals(result["field_2"]["sub_field_0"], 123)
        self.assertEquals(result["field_2"]["sub_field_1"], "a")
        self.assertEquals(result["field_0"]["sub_field_0"]["sub_sub_field_test"], 456)

    def test_update_in_existing_file(self):

        self._logger.info(
            "Updating existing fields (one is a primitive, other is a list)"
        )
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=["field_0.sub_field_0=abc", "field_0.sub_field_1=aa,bb"],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(result["field_0"]["sub_field_0"], "abc")
        self.assertEquals(result["field_0"]["sub_field_1"], ["aa", "bb"])

        self._logger.info("Updating a non-existing field")
        self.assertRaisesRegexp(
            RuntimeError,
            "Key not found in dict",
            self._relayer.relayer_config,
            add_kvs=None,
            rm_keys=None,
            update_kvs=["field_0.sub_field_3=abc"],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

    def test_update_in_dotted_path(self):
        self._logger.info("Updating an existing field where path is dotted (1/2)")
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=[
                r"dotted\.field.internal_field[0].list\.item_0\.key1=modified",
                r"dotted\.field.internal_field[1].list\.item_1\.key2=6",
            ],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        # modified
        self.assertEquals(
            result["dotted.field"]["internal_field"][0]["list.item_0.key1"], "modified"
        )
        self.assertEquals(
            result["dotted.field"]["internal_field"][1]["list.item_1.key2"], 6
        )

        # unmodified
        self.assertEquals(result["field_0"]["sub_field_1"], "r3lay3r")

        self.assertEquals(
            result["dotted.field"]["internal_field"][0]["list.item_0.key2"], "value02"
        )
        self.assertEquals(
            result["dotted.field"]["internal_field"][1]["list.item_1.key1"], "value11"
        )

        self._logger.info("Updating an existing field where path is dotted (2/2)")
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=[
                r"dont_open_dotted_inside.internal\.dotted\.field."
                r"internal_1[0].list_item_0.internal_2.deep\.dotted\.field=modified",
            ],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        # modified
        self.assertEquals(
            result["dont_open_dotted_inside"]["internal.dotted.field"]["internal_1"][0][
                "list_item_0"
            ]["internal_2"]["deep.dotted.field"],
            "modified",
        )

        # unmodified
        self.assertEquals(result["field_0"]["sub_field_1"], "r3lay3r")
        self.assertEquals(
            result["dotted.field"]["internal_field"][1]["list.item_1.key1"], "value11"
        )

    def test_update_from_aux_file(self):

        # update existing fields (one is a primitive, other is a list)
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=self._aux_file_path,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        # field_0 unchanged
        self.assertEquals(result["field_0"], self._original_config_dict["field_0"])

        # field_1 modified
        self.assertEquals(
            result["field_1"]["sub_field_0"]["sub_sub_field_0"],
            self._aux_config_dict["field_1"]["sub_field_0"]["sub_sub_field_0"],
        )

        self.assertEquals(
            result["field_1"]["sub_field_0"]["sub_sub_field_1"],
            self._original_config_dict["field_1"]["sub_field_0"]["sub_sub_field_1"],
        )

        # field_2 added
        self.assertEquals(result["field_2"], self._aux_config_dict["field_2"])

    def test_removal_from_existing_file(self):

        # remove existing fields (one a primitive, other a list)
        self.assertIn(
            "sub_sub_field_0", self._original_config_dict["field_1"]["sub_field_0"]
        )

        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=["field_1.sub_field_0.sub_sub_field_0", "field_0.sub_field_2"],
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertNotIn("sub_sub_field_0", result["field_1"]["sub_field_0"])
        self.assertNotIn("sub_field_2", result["field_0"])

        # remove a non-existing field
        self.assertRaisesRegexp(
            RuntimeError,
            "Subsection not found in dict",
            self._relayer.relayer_config,
            add_kvs=None,
            rm_keys=["field0.sub_field_3"],
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        # remove key with value
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=["field_1.sub_field_0.sub_sub_field_1=aa"],
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertNotIn("sub_sub_field_1", result["field_1"]["sub_field_0"])

    def test_implicit_value_type_casting(self):

        # update existing fields with different value types and check conversion
        self._relayer.relayer_config(
            add_kvs=[
                "interesting_field.sub_field_0=abc",
                "interesting_field.sub_field_1=543",
                "interesting_field.sub_field_2=59.959",
                "interesting_field.sub_field_3=t",
                "interesting_field.sub_field_4=true",
                "interesting_field.sub_field_5=f",
                "interesting_field.sub_field_6=FALSE",
                "interesting_field.sub_field_7=aa,bb",
                "interesting_field.sub_field_8=aa,34.6,t,bb",
                "interesting_field.sub_field_9={bb:aa, dd:22.2},{kk: 1},{aa:t}",
            ],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(result["interesting_field"]["sub_field_0"], "abc")
        self.assertEquals(result["interesting_field"]["sub_field_1"], 543)
        self.assertEquals(result["interesting_field"]["sub_field_2"], 59.959)
        self.assertEquals(result["interesting_field"]["sub_field_3"], True)
        self.assertEquals(result["interesting_field"]["sub_field_4"], True)
        self.assertEquals(result["interesting_field"]["sub_field_5"], False)
        self.assertEquals(result["interesting_field"]["sub_field_6"], False)
        self.assertEquals(result["interesting_field"]["sub_field_7"], ["aa", "bb"])
        self.assertEquals(
            result["interesting_field"]["sub_field_8"], ["aa", 34.6, True, "bb"]
        )
        self.assertEquals(
            result["interesting_field"]["sub_field_9"],
            [{"bb": "aa", "dd": 22.2}, {"kk": 1}, {"aa": True}],
        )

    def test_new_non_leaf_list(self):
        """
        We're gonna create a list of strings and then modify one of the values for be a dict
        results:
            field_0:
              sub_field_3:
                - 'relay3_0'
                - relay3_1:
                    relay3_1_1: 5
                    relay3_1_2: '8.8.8.8'
                - 'relay3_2'
        """

        # add a totally new field
        self._relayer.relayer_config(
            add_kvs=["field_0.sub_field_3=relay3_0,relay3_1,relay3_2"],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(
            result["field_0"]["sub_field_3"], ["relay3_0", "relay3_1", "relay3_2"]
        )

        # add a sub-fields to one of the lists text fields, to coerce it into a dict
        self._relayer.relayer_config(
            add_kvs=[
                "field_0.sub_field_3.relay3_1.relay3_1_1=5",
                "field_0.sub_field_3.relay3_1.relay3_1_2=8.8.8.8",
            ],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        # the other list elements we added are still there
        self.assertTrue("relay3_0" in result["field_0"]["sub_field_3"])
        self.assertTrue("relay3_2" in result["field_0"]["sub_field_3"])
        self.assertEquals(
            result["field_0"]["sub_field_3"][1],
            {"relay3_1": {"relay3_1_1": 5, "relay3_1_2": "8.8.8.8"}},
        )

        # Create list with empty items
        self._relayer.relayer_config(
            add_kvs=["field_0.sub_field_7=item,", "field_0.sub_field_8=a,,b"],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)
        self.assertEqual(result["field_0"]["sub_field_7"], ["item"])
        self.assertEqual(result["field_0"]["sub_field_8"], ["a", "b"])

    def test_mutate_existing_non_leaf_list(self):
        """
        We're gonna modify one of the values in an existing list for it to be a dict
        results:
            field_0:
              sub_field_0: 1024
              sub_field_1: 'r3lay3r'
              sub_field_2:
                - relay0_0:
                    relay0_0_0:
                        shi: 'kaka'
                - 'relay0_1'
                - 'relay0_2'
        """

        # add a totally new field
        self._relayer.relayer_config(
            add_kvs=["field_0.sub_field_2.relay0_0.relay0_0_0.shi=kaka"],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self._logger.debug("result", result=result)

        expected_inner_list = [
            {"relay0_0": {"relay0_0_0": {"shi": "kaka"}}},
            "relay0_1",
            "relay0_2",
        ]
        self.assertEquals(result["field_0"]["sub_field_2"], expected_inner_list)

        # the other list elements we added are still there
        self.assertEquals(result["field_0"]["sub_field_0"], 1024)
        self.assertEquals(result["field_0"]["sub_field_1"], "r3lay3r")

    def test_add_item_in_list(self):

        # change item in list
        self._add_to_list("field_0.sub_field_2", "1", "change")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(result["field_0"]["sub_field_2"][1], "change")

        # append item in list
        self._add_to_list("field_0.sub_field_2", "end", "append")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(len(result["field_0"]["sub_field_2"]), 4)
        self.assertEquals(result["field_0"]["sub_field_2"][3], "append")

        # insert item in start of list
        self._add_to_list("field_0.sub_field_2", "start", "insert_before")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(len(result["field_0"]["sub_field_2"]), 5)
        self.assertEquals(result["field_0"]["sub_field_2"][0], "insert_before")

        # assert full list equality
        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            ["insert_before", "relay0_0", "change", "relay0_2", "append"],
        )

        # new list
        self._add_to_list("field_0.does_not_exist", "0", "new")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(result["field_0"]["does_not_exist"], ["new"])

    def test_update_list_item_property(self):
        self._update_list_item_property("field_2.sub_field_0", "1", "data", "1")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_2"]["sub_field_0"],
            [
                {"name": "a", "data": 0},
                {"name": "b", "data": 1},
                {"name": "c", "data": 0},
            ],
        )

        self._update_list_item_property("field_2.sub_field_0", "start", "data", "2")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_2"]["sub_field_0"],
            [
                {"name": "a", "data": 2},
                {"name": "b", "data": 1},
                {"name": "c", "data": 0},
            ],
        )

        # root is a list
        self._update_list_item_property("list_field", "start", "data", "2")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEqual(result["list_field"][0]["data"], 2)

        # a bit more complex than the last checks
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=["list_field[1].data.sub_data[start].val=sub_val_3"],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertDictEqual(
            result["list_field"][1]["data"]["sub_data"][0],
            {"attr": "sub_a", "val": "sub_val_3"},
        )
        self.assertDictEqual(
            result["list_field"][1]["data"]["sub_data"][1],
            {"attr": "sub_b", "val": "sub_val_2"},
        )

        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=["list_field[2].data[0].val=h"],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertDictEqual(
            result["list_field"][2]["data"][0], {"attr": "d", "val": "h"}
        )
        self.assertDictEqual(
            result["list_field"][2]["data"][1], {"attr": "f", "val": "g"}
        )

    def test_insert_item_in_list(self):

        # change item in list
        self._add_to_list("field_0.sub_field_2", "1", "insert", insert=True)
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(len(result["field_0"]["sub_field_2"]), 4)
        self.assertEquals(result["field_0"]["sub_field_2"][1], "insert")

        # insert item in start of list
        self._add_to_list("field_0.sub_field_2", "0", "insert_before", insert=True)
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(len(result["field_0"]["sub_field_2"]), 5)
        self.assertEquals(result["field_0"]["sub_field_2"][0], "insert_before")

        # assert full list equality
        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            ["insert_before", "relay0_0", "insert", "relay0_1", "relay0_2"],
        )

    def test_fail_add_and_insert_item_in_list(self):
        def _assert_add_and_insert(exception, key, idx, value):
            self.assertRaises(exception, self._add_to_list, key, idx, value)
            self.assertRaises(
                exception, self._add_to_list, key, idx, value, insert=True
            )

        # change item in list with string index that's not start or end
        _assert_add_and_insert(TypeError, "field_0.sub_field_2", "a", "change")

        # change item in key that isn't a list
        _assert_add_and_insert(RuntimeError, "field_0.sub_field_0", "1", "change")

        # change item in list - index out of range
        _assert_add_and_insert(IndexError, "field_0.sub_field_2", "5", "change")

        # change item in list that doesn't exist
        _assert_add_and_insert(RuntimeError, "field_0.does_not_exist", "1", "change")

    def test_extend_list(self):

        # extend list to the end with brackets
        self._extend_list("field_0.sub_field_2", "", "extend,this")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            ["relay0_0", "relay0_1", "relay0_2", "extend", "this"],
        )

        # extend single to the end with brackets
        self._extend_list("field_0.sub_field_2", "", "single")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            ["relay0_0", "relay0_1", "relay0_2", "extend", "this", "single"],
        )

        # extend single to the start
        self._extend_list("field_0.sub_field_2", "start", "single")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            ["single", "relay0_0", "relay0_1", "relay0_2", "extend", "this", "single"],
        )

        # extend list to the end without brackets
        self._extend_list("field_0.sub_field_2", "", "and,this", brackets=False)
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            [
                "single",
                "relay0_0",
                "relay0_1",
                "relay0_2",
                "extend",
                "this",
                "single",
                "and",
                "this",
            ],
        )

        # extend list to the beginning
        self._extend_list("field_0.sub_field_2", "start", "this,before")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_0"]["sub_field_2"],
            [
                "this",
                "before",
                "single",
                "relay0_0",
                "relay0_1",
                "relay0_2",
                "extend",
                "this",
                "single",
                "and",
                "this",
            ],
        )

        # extend list in middle
        self._extend_list(
            "field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0",
            "1",
            "middle,between",
        )
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_1"]["sub_field_0"]["sub_sub_field_1"]["sub_sub_sub_field_0"],
            ["relay1_0", "middle", "between", "relay1_1", "relay1_2"],
        )

    def test_fail_extend_in_list(self):

        # extend item in key that doesn't exist
        self.assertRaises(
            RuntimeError, self._extend_list, "field_0.does_not_exist", "", "change"
        )

        # extend item in key that isn't a list
        self.assertRaises(
            RuntimeError, self._extend_list, "field_0.sub_field_0", "", "change"
        )

    def test_remove_from_list(self):

        # remove from list by index
        self._remove_from_list("field_0.sub_field_2", "1")
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(result["field_0"]["sub_field_2"], ["relay0_0", "relay0_2"])

        # remove from list by value
        self._remove_from_list("field_0.sub_field_2", "relay0_2", by_value=True)
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(result["field_0"]["sub_field_2"], ["relay0_0"])

        # remove from list by value (json)
        self._remove_from_list(
            "field_2.sub_field_0", '{"name": "a", "data": 0}', by_value=True
        )
        result = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertListEqual(
            result["field_2"]["sub_field_0"],
            [
                {"name": "b", "data": 0},
                {"name": "c", "data": 0},
            ],
        )

        # remove item from list by value - item doesn't exist, quiet failure
        self._remove_from_list("field_0.sub_field_2", "does_not_exist", by_value=True)

    def test_fail_remove_item_from_list(self):

        # remove item from list with string index that's not start or end
        self.assertRaises(TypeError, self._remove_from_list, "field_0.sub_field_2", "a")

        # remove item from key that isn't a list
        self.assertRaises(
            RuntimeError, self._remove_from_list, "field_0.sub_field_0", "1"
        )

        # remove item from list - index out of range
        self.assertRaises(
            IndexError, self._remove_from_list, "field_0.sub_field_2", "5"
        )

        # remove item from list that doesn't exist
        self.assertRaises(
            RuntimeError, self._remove_from_list, "field_0.does_not_exist", "1"
        )

        # remove item from list that doesn't exist (by value)
        self.assertRaises(
            RuntimeError,
            self._remove_from_list,
            "field_0.does_not_exist",
            "value",
            by_value=True,
        )

    def test_ignore_not_found_remove_item_from_list(self):
        last_modified_time = os.path.getmtime(self._modifiable_file_path)

        # remove item from list that doesn't exist
        self._remove_from_list("field_0.does_not_exist", "1", ignore_not_found=True)

        # remove item from list - index out of range
        self._remove_from_list("field_0.sub_field_2", "5", ignore_not_found=True)

        # remove item from key that isn't a list - should still raise
        self.assertRaises(
            RuntimeError,
            self._remove_from_list,
            "field_0.sub_field_0",
            "1",
            ignore_not_found=True,
        )

        # remove item from list that doesn't exist (by value)
        self._remove_from_list(
            "field_0.does_not_exist",
            "value",
            by_value=True,
            ignore_not_found=True,
        )

        # assert file was not modified
        self.assertEquals(
            last_modified_time, os.path.getmtime(self._modifiable_file_path)
        )

    def _add_to_list(self, key, idx, value, insert=False):
        query_str = "{key}[{idx}]={value}".format(key=key, idx=idx, value=value)
        self._relayer.relayer_config(
            add_kvs=None if insert else [query_str],
            rm_keys=None,
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None if not insert else [query_str],
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

    def _update_list_item_property(self, key, idx, item_property, value):
        query_str = "{key}[{idx}].{item_property}={value}".format(
            key=key, idx=idx, item_property=item_property, value=value
        )
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=[query_str],
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

    def _extend_list(self, key, idx, value, brackets=True):
        if brackets:
            query_str = "{key}[{idx}]={value}".format(key=key, idx=idx, value=value)
        else:
            query_str = "{key}={value}".format(key=key, value=value)

        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None,
            update_kvs=None,
            extend_kvs=[query_str],
            insert_kvs=None,
            rm_list_element_keys=None,
            file_path_to_merge=None,
        )

    def _remove_from_list(self, key, idx, by_value=False, ignore_not_found=False):
        query_str = "{key}[{idx}]".format(key=key, idx=idx)
        self._relayer.relayer_config(
            add_kvs=None,
            rm_keys=None if by_value else [query_str],
            update_kvs=None,
            extend_kvs=None,
            insert_kvs=None,
            rm_list_element_keys=None if not by_value else [query_str],
            file_path_to_merge=None,
            ignore_not_found=ignore_not_found,
        )

    @staticmethod
    def _relayer_config_yml_to_dict(config_path):
        return yaml.safe_load(open(config_path))
