import os
import shutil
from ruamel import yaml

from twisted.internet import defer

import tests.integration
import framework.common.helpers

"""
This integration test uses zebo's test infrastructure
"""


class RelayerTestCase(tests.integration.TestCase):
    def __init__(self, *args, **kwargs):
        super(RelayerTestCase, self).__init__(*args, **kwargs)
        self._modifiable_file_path = None

    def requirements(self):
        return self.base_requirements() + ["relayer_controller"]

    def set_up(self):
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

        self._modifiable_file_path = get_path_to_file(modifiable_file_name)

        # auxiliary file (for --from-file updates)
        self._aux_file_path = get_path_to_file("relayer_aux_test.yml")

        # make a modifiable copy of the yml fixture
        shutil.copyfile(original_file_path, self._modifiable_file_path)

        self._original_fields_dict = self._relayer_config_yml_to_dict(
            self._modifiable_file_path
        )
        self._aux_config_dict = self._relayer_config_yml_to_dict(self._aux_file_path)

    def tear_down(self):
        if self._modifiable_file_path:
            os.unlink(self._modifiable_file_path)

    @defer.inlineCallbacks
    def test_all_operations(self):

        # basic check
        list_of_dicts_to_add = (
            r"{type:a,value:2},{type:b,value:t},{type:c,value:aa},"
            r"{type:d.with.dots,dot.value:4}"
        )

        yield self._run_relayer(
            self._modifiable_file_path,
            args_to_add=[
                r"field_2.sub_field_0=123",
                r"field_0.sub_field_3=aa",
                r"field_0.sub_field_5={0}".format(list_of_dicts_to_add),
                r"field_1.sub_field_1.dotted\.sub\.field_0=new",
            ],
            args_to_update=[
                r"field_0.sub_field_1=456,54",
                r"field_1.sub_field_0.dotted\.sub\.field_2=modified",
            ],
            args_to_remove=[
                r"field_0.sub_field_2",
                r"field_1.sub_field_0.sub_sub_field_0",
                r"field_1.sub_field_0.dotted\.sub\.field_3",
                r"field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0[0]",
            ],
            args_to_remove_from_list=[
                r"field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0[relay1_1]",
            ],
        )

        resulting_config = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(resulting_config["field_2"]["sub_field_0"], 123)
        self.assertEquals(resulting_config["field_0"]["sub_field_3"], "aa")
        self.assertEquals(
            resulting_config["field_0"]["sub_field_5"],
            [
                {"type": "a", "value": 2},
                {"type": "b", "value": True},
                {"type": "c", "value": "aa"},
                {"type": "d.with.dots", "dot.value": 4},
            ],
        )
        self.assertEquals(
            resulting_config["field_1"]["sub_field_1"]["dotted.sub.field_0"], "new"
        )
        self.assertEquals(resulting_config["field_0"]["sub_field_1"], [456, 54])
        self.assertEquals(
            resulting_config["field_1"]["sub_field_0"]["dotted.sub.field_2"], "modified"
        )
        self.assertNotIn("sub_field_2", resulting_config["field_0"])
        self.assertNotIn("sub_sub_field_0", resulting_config["field_1"]["sub_field_0"])
        self.assertNotIn(
            "dotted.sub.field_3", resulting_config["field_1"]["sub_field_0"]
        )
        self.assertNotIn(
            "relay1_0",
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_1"][
                "sub_sub_sub_field_0"
            ],
        )
        self.assertNotIn(
            "relay1_1",
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_1"][
                "sub_sub_sub_field_0"
            ],
        )
        self.assertIn(
            "relay1_2",
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_1"][
                "sub_sub_sub_field_0"
            ],
        )

    @defer.inlineCallbacks
    def test_ignore_not_found(self):

        yield self._run_relayer(
            self._modifiable_file_path,
            args_to_add=[
                r"field_2.sub_field_0=123",
                r"field_1.sub_field_1.dotted\.sub\.field_0=new",
            ],
            args_to_update=[
                r"field_0.sub_field_1=456,54",
                r"field_1.sub_field_0.dotted\.sub\.field_2=modified",
            ],
            args_to_remove=[
                r"field_0.sub_field_2",
                r"field_0.does_not_exist",
                r"field_1.sub_field_0.dotted\.sub\.field_3",
                r"field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0[0]",
                r"field_1.sub_field_0.sub_sub_field_1.does_not_exist[0]",
            ],
            args_to_remove_from_list=[
                r"field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0[relay1_1]",
                r"field_1.sub_field_0.sub_sub_field_1.does_not_exist[relay1_1]",
            ],
            ignore_not_found=True,
        )

        resulting_config = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(resulting_config["field_2"]["sub_field_0"], 123)
        self.assertEquals(
            resulting_config["field_1"]["sub_field_1"]["dotted.sub.field_0"], "new"
        )
        self.assertEquals(resulting_config["field_0"]["sub_field_1"], [456, 54])
        self.assertEquals(
            resulting_config["field_1"]["sub_field_0"]["dotted.sub.field_2"], "modified"
        )
        self.assertNotIn("sub_field_2", resulting_config["field_0"])
        self.assertNotIn(
            "dotted.sub.field_3", resulting_config["field_1"]["sub_field_0"]
        )
        self.assertNotIn(
            "relay1_0",
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_1"][
                "sub_sub_sub_field_0"
            ],
        )
        self.assertNotIn(
            "relay1_1",
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_1"][
                "sub_sub_sub_field_0"
            ],
        )

        with self.assertRaises(framework.common.helpers.CommandFailedError):
            yield self._run_relayer(
                self._modifiable_file_path,
                args_to_add=[
                    r"field_2.sub_field_0=123",
                    r"field_1.sub_field_1.dotted\.sub\.field_0=new",
                ],
                args_to_update=[
                    r"field_0.sub_field_1=456,54",
                    r"field_1.sub_field_0.dotted\.sub\.field_2=modified",
                ],
                args_to_remove=[
                    r"field_0.does_not_exist",
                ],
                ignore_not_found=False,
            )

        with self.assertRaises(framework.common.helpers.CommandFailedError):
            yield self._run_relayer(
                self._modifiable_file_path,
                args_to_remove=[
                    r"field_1.sub_field_0.sub_sub_field_1.does_not_exist[0]",
                ],
                ignore_not_found=False,
            )

        with self.assertRaises(framework.common.helpers.CommandFailedError):
            yield self._run_relayer(
                self._modifiable_file_path,
                args_to_remove_from_list=[
                    r"field_1.sub_field_0.sub_sub_field_1.sub_sub_sub_field_0[relay1_1]",
                    r"field_1.sub_field_0.sub_sub_field_1.does_not_exist[relay1_1]",
                ],
                ignore_not_found=False,
            )

    @defer.inlineCallbacks
    def test_all_and_file_merge(self):
        yield self._run_relayer(
            self._modifiable_file_path,
            args_to_add=["field_3.sub_field_0=123", "field_0.sub_field_3=aa"],
            args_to_update=["field_0.sub_field_1=456,54"],
            args_to_remove=["field_0.sub_field_2"],
            arg_to_file_merge=self._aux_file_path,
        )

        resulting_config = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(resulting_config["field_0"]["sub_field_3"], "aa")
        self.assertEquals(resulting_config["field_3"]["sub_field_0"], 123)
        self.assertEquals(resulting_config["field_0"]["sub_field_1"], [456, 54])
        self.assertNotIn("sub_field_2", resulting_config["field_0"])

        # from the merge aux config
        self.assertIn("sub_sub_field_0", resulting_config["field_1"]["sub_field_0"])
        self.assertEquals(
            resulting_config["field_2"]["sub_field_0"]["sub_sub_field_0"], "ab"
        )
        self.assertEquals(
            resulting_config["field_1"]["sub_field_0"]["sub_sub_field_0"], "ab"
        )

    @defer.inlineCallbacks
    def test_yaml_extension_agnosticism(self):

        # should work even though the file is .yml and not .yaml
        yield self._run_relayer(
            self._modifiable_file_path.replace(".yml", ".yaml"),
            args_to_add=["field_2.sub_field_0=123"],
        )

        resulting_config = self._relayer_config_yml_to_dict(self._modifiable_file_path)

        self.assertEquals(resulting_config["field_2"]["sub_field_0"], 123)

    @defer.inlineCallbacks
    def _run_relayer(
        self,
        config_path,
        args_to_add=None,
        args_to_update=None,
        args_to_remove=None,
        args_to_remove_from_list=None,
        arg_to_file_merge=None,
        ignore_not_found=False,
    ):

        # relayer execution path
        relayer_path = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../relayer")
        )
        command = "{0} -c {1}".format(relayer_path, config_path)

        if args_to_add:
            for arg_to_add in args_to_add:
                command += ' -a "{0}"'.format(arg_to_add)

        if args_to_update:
            for arg_to_update in args_to_update:
                command += ' -u "{0}"'.format(arg_to_update)

        if args_to_remove:
            for arg_to_remove in args_to_remove:
                command += ' -r "{0}"'.format(arg_to_remove)

        if args_to_remove_from_list:
            for arg_to_remove_from_list in args_to_remove_from_list:
                command += ' -rl "{0}"'.format(arg_to_remove_from_list)

        if arg_to_file_merge:
            command += " -ff {0}".format(arg_to_file_merge)

        if ignore_not_found:
            command += " -inf"
        self._logger.info("Running relayer command", comamnd=command)
        yield framework.common.helpers.run_command(command, logger=self._logger)

    @staticmethod
    def _relayer_config_yml_to_dict(config_path):
        with open(config_path, "r") as f:
            return yaml.load(f, Loader=yaml.Loader)
