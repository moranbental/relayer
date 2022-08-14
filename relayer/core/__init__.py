#!/usr/bin/env python

import os
import re
import sys
import enum
import simplejson

from ruamel import yaml

from . import helpers


class Relayer(object):
    class KeyOperations(enum.Enum):
        Remove = 0
        Update = 1
        Add = 2
        RemoveListElement = 3
        ExtendList = 4
        InsertToList = 5

        @classmethod
        def all_remove_operations(cls):
            return [cls.Remove, cls.RemoveListElement]

    class ListIndices(object):
        start = "start"
        end = "end"

    def __init__(self, logger, config_path, debug=False):
        self._logger = logger
        self._config_path = config_path
        self._debug = debug

    def relayer_config(
        self,
        add_kvs,
        rm_keys,
        update_kvs,
        extend_kvs,
        insert_kvs,
        rm_list_element_keys,
        file_path_to_merge,
        ignore_not_found=False,
    ):
        merge_changed = False

        resolved_config_path = self._resolve_config_path(self._config_path)
        config = self._load_config(resolved_config_path)
        config, rm_changed = self._mod_kvs(
            config,
            rm_keys or [],
            operation=self.KeyOperations.Remove,
            ignore_not_found=ignore_not_found,
        )
        config, rm_list_changed = self._mod_kvs(
            config,
            rm_list_element_keys or [],
            operation=self.KeyOperations.RemoveListElement,
            ignore_not_found=ignore_not_found,
        )
        config, up_changed = self._mod_kvs(
            config, update_kvs or [], operation=self.KeyOperations.Update
        )
        config, add_changed = self._mod_kvs(
            config, add_kvs or [], operation=self.KeyOperations.Add
        )
        config, extend_changed = self._mod_kvs(
            config, extend_kvs or [], operation=self.KeyOperations.ExtendList
        )
        config, insert_changed = self._mod_kvs(
            config, insert_kvs or [], operation=self.KeyOperations.InsertToList
        )

        if file_path_to_merge:
            config, merge_changed = self._merge_configs(config, file_path_to_merge)

        changed = any(
            [
                rm_changed,
                rm_list_changed,
                up_changed,
                add_changed,
                extend_changed,
                insert_changed,
                merge_changed,
            ]
        )
        if not changed:
            self._logger.warn(
                "No changes to configuration, not overwriting file",
                config_path=resolved_config_path,
            )
            return

        self._dump_config(config, resolved_config_path)

    def _resolve_config_path(self, original_config_path):
        """
        A trick for maintaining cross-version compatibility while we move config ext. .yml -> .yaml
        Relayer will be agnostic to .yml/.yaml extension and will try both before failing
        """
        optional_config_paths = [original_config_path]
        config_path_root, ext = os.path.splitext(original_config_path)
        if ext == ".yml":
            optional_config_paths.append(config_path_root + ".yaml")
        elif ext == ".yaml":
            optional_config_paths.append(config_path_root + ".yml")
        else:
            self._logger.debug(
                "Configuration file provided is not a standard YAML extension, "
                "not inferring extension variants",
                config_path=original_config_path,
            )
            return original_config_path

        self._logger.debug(
            "Optional configuration paths resolved",
            optional_config_paths=optional_config_paths,
        )
        for path in optional_config_paths:
            if os.path.isfile(path):
                self._logger.info("Found configuration file in path", path=path)
                return path

            self._logger.info("Config file not found in path", path=path)

        raise RuntimeError("Failed to find configuration file in provided path")

    def _load_config(self, config_path):
        """
        Reading a yaml compatible config file
        """
        self._logger.info("Attempting to load config", config_path=config_path)
        try:
            with open(config_path, "r") as f:
                config = yaml.round_trip_load(f, preserve_quotes=True) or {}
                self._logger.info(
                    "Configuration file loaded successfully", config_path=config_path
                )
        except yaml.YAMLError as exc:
            self._logger.error(
                "Failed to parse configuration file, malformed config.",
                config=config_path,
                exc=str(exc),
            )
            raise
        except Exception as exc:
            self._logger.error(
                "Failed to read configuration file", config=config_path, exc=str(exc)
            )
            raise

        self._logger.debug("Config loaded", config=config)
        return config

    def _dump_config(self, config, config_path):
        if self._debug:
            self._logger.debug("Dumping modified configuration to stdout")
            yaml.round_trip_dump(config, sys.stdout)
            return

        self._logger.debug(
            "Dumping modified configuration to file", config_path=config_path
        )
        with open(config_path, "w") as fh:
            yaml.round_trip_dump(config, fh)

    def _mod_kvs(self, config, kvs, operation=None, ignore_not_found=False):
        mod_key_kwargs = {
            "append_mode": False,
            "extend_mode": False,
            "list_insert_mode": False,
            "rm_mode": False,
            "rm_value_mode": False,
            "ignore_not_found": ignore_not_found,
        }
        changed = False

        if operation == self.KeyOperations.Update:
            self._logger.info("Updating requested keys", kvs=kvs)
        elif operation == self.KeyOperations.Add:
            self._logger.info("Adding requested keys", kvs=kvs)
            mod_key_kwargs["append_mode"] = True
        elif operation == self.KeyOperations.ExtendList:
            self._logger.info("Extending requested list keys", kvs=kvs)
            mod_key_kwargs["append_mode"] = True
            mod_key_kwargs["extend_mode"] = True
        elif operation == self.KeyOperations.InsertToList:
            self._logger.info("Inserting requested keys", kvs=kvs)
            mod_key_kwargs["append_mode"] = True
            mod_key_kwargs["list_insert_mode"] = True
        elif operation == self.KeyOperations.Remove:
            self._logger.info("Removing requested keys", kvs=kvs)
            mod_key_kwargs["append_mode"] = False
            mod_key_kwargs["rm_mode"] = True
        elif operation == self.KeyOperations.RemoveListElement:
            self._logger.info("Removing requested values from keys", kvs=kvs)
            mod_key_kwargs["append_mode"] = False
            mod_key_kwargs["rm_mode"] = True
            mod_key_kwargs["rm_value_mode"] = True
        else:
            raise RuntimeError("Unknown key operation: {0}".format(operation))

        for kv in kvs:
            if operation in self.KeyOperations.all_remove_operations():

                # accidental '=' in arg? ignore trailing chars
                key = kv.split("=", 1)[0] if "=" in kv else kv
                value = None
            else:
                key, value = kv.split("=", 1)

            # if value is actually a list (or dict), build a python list (or dict or list of dicts) out of it
            if value is not None and ("," in value or "{" in value):
                if "{" in value:
                    try:
                        converted_value = []

                        # value is a dict or list of dicts (leave just the inner values,
                        # without the surrounding parenthesis)
                        value_dicts = [
                            split_value.split("{")[1]
                            for split_value in value.split("}")
                            if split_value
                        ]
                        for value_dict in value_dicts:

                            # for each "dict" construct a real dict and add it to the list
                            # keep a list of keys and a list of lists of values
                            keys = []
                            values = []
                            real_value_dict = {}

                            # split all the commas, and infer what are keys and what are values
                            comma_separated_string = value_dict.split(",")
                            for key_or_values in comma_separated_string:

                                # if it got : in the string, split and take the first item as key
                                if ":" in key_or_values:
                                    kv_split = key_or_values.split(":", 1)
                                    keys.append(kv_split[0])

                                    # the string after : and before , is a value, create a list of values with
                                    # only one item for now, the else will append more items to it
                                    if len(kv_split) > 1:
                                        values.append(kv_split[1:])
                                else:

                                    # all other stuff are values (may be lists that were split)
                                    # important to place them in the right index
                                    values[-1].append(key_or_values)

                            for k, v in zip(keys, values):
                                # if we got a list of values for the key, pass it as is, else take the only item
                                real_value_dict[k] = v if len(v) > 1 else v[0]

                            converted_value.append(real_value_dict)

                        value = converted_value
                    except Exception:
                        raise Exception(
                            "Wrong value syntax - expected either a dictionary or a list of dictionaries"
                        )
                else:
                    try:

                        # value is a simple list
                        value = list(filter(lambda item: len(item), value.split(",")))
                    except Exception:
                        raise Exception(
                            "Wrong value syntax - expected a comma separated list"
                        )

            # now convert the value to yaml
            value = helpers.convert_value_to_yaml(value)

            config, op_changed = self._modify_key(config, key, value, **mod_key_kwargs)
            changed = changed | op_changed
        return config, changed

    @staticmethod
    def _enrich_level_index(level):
        list_index_re = re.match("(?P<level>.*)\\[(?P<idx>.*)\\]", level)
        if list_index_re is not None:
            level = list_index_re.group("level")
            idx = list_index_re.group("idx")
            if idx.isdigit():
                return level, int(idx)
            return level, idx
        return level, None

    def _modify_key(
        self,
        config,
        full_key,
        value,
        append_mode=True,
        list_insert_mode=False,
        extend_mode=False,
        rm_mode=False,
        rm_value_mode=False,
        ignore_not_found=False,
    ):
        """
        Adds a key to the config. if exists overrides, else adds
        """

        def _get_subsection(section, key, section_idx):
            """
            An auxiliary function
            """
            if isinstance(section, dict):
                if key in section:
                    return section[key], None
            if isinstance(section, list):
                if section_idx == self.ListIndices.start:
                    section_idx = 0
                elif section_idx == self.ListIndices.end:
                    section_idx = len(section) - 1
                for idx, element in enumerate(section):
                    if element == key and (idx == section_idx or section_idx is None):
                        return section[idx], idx

                    # it's a key in a 1-dict element in the list
                    if (
                        isinstance(element, dict)
                        and key in element
                        and (idx == section_idx or section_idx is None)
                    ):
                        return section[idx][key], idx
            return None, None

        def _assign_subsection(section, key, subsection, section_idx=None):
            """
            An auxiliary function
            """
            if isinstance(section, dict):
                if key in section:
                    section[key] = subsection
            elif isinstance(section, list):
                if section_idx == self.ListIndices.start:
                    section_idx = 0
                elif section_idx == self.ListIndices.end:
                    section_idx = len(section) - 1
                for idx, element in enumerate(section):
                    if element == key and (idx == section_idx or section_idx is None):
                        section[idx] = subsection

                    # it's a key in a 1-dict element in the list
                    if (
                        isinstance(element, dict)
                        and key in element
                        and (idx == section_idx or section_idx is None)
                    ):
                        section[idx][key] = subsection
            return section

        def _handle_extend_list(section, level, level_idx):
            extend_value = helpers.as_list(value)
            self._logger.debug("Extending list", list=level, idx=level_idx)
            if level_idx == self.ListIndices.start:
                extend_value.extend(section[level])
                return extend_value
            elif isinstance(level_idx, int):
                section[level][level_idx:level_idx] = extend_value
            elif level_idx is None or level_idx == "":
                section[level].extend(extend_value)
            else:
                raise ValueError("List index has invalid value: {0}".format(level_idx))

        def _handle_add_to_list(section, level, level_idx):

            # extend list with brackets
            if extend_mode:

                # directly insert a new extended list by returning it
                return _handle_extend_list(section, level, level_idx)

            # append to list
            if level_idx == self.ListIndices.start:
                self._logger.debug(
                    "Appending to start of list", list=level, idx=level_idx
                )
                section[level].insert(0, value)
                return
            if level_idx == self.ListIndices.end:
                self._logger.debug(
                    "Appending to end of list", list=level, idx=level_idx
                )
                section[level].append(value)
                return

            # insert to list
            if list_insert_mode:
                if isinstance(level_idx, int) and level_idx > len(section[level]):
                    self._logger.warn("index out of range", key=level, idx=level_idx)
                    raise IndexError("index out of range")
                self._logger.debug("Inserting to list", list=level, idx=level_idx)
                section[level].insert(level_idx, value)
                return

            # change item in list
            self._logger.debug("Setting value on a list", list=level, idx=level_idx)
            section[level][level_idx] = value

        def _handle_insert_in_section(section, level, level_idx, scope):
            if level_idx is not None or extend_mode or list_insert_mode:
                if not isinstance(section[level], list):
                    self._logger.warn(
                        "Key is not a list", key=level, at=".".join(scope)
                    )
                    raise RuntimeError("Key is not a list")

                return _handle_add_to_list(section, level, level_idx)

            # extend list without brackets
            if extend_mode:
                return _handle_extend_list(section, level, level_idx)

            # direct insert value (not list) by returning
            self._logger.debug(
                "Setting value on an existing leaf", key=level, value=value
            )
            return value

        def _handle_rm_by_value(section, level, rm_value):

            # try loading value as json dict for complex value removals
            try:
                rm_value = simplejson.loads(rm_value)
            except ValueError:

                # value isn't json
                if rm_value not in section[level]:
                    self._logger.info("Element not found in list", element=rm_value)
                    return

                self._logger.debug(
                    "Removing element from list", key=level, element=rm_value
                )
                section[level].remove(rm_value)

            else:

                # sections are ruamel.yaml objects and won't allow us to just remove a dictionary from them
                for idx, item in enumerate(section[level]):
                    if rm_value == item:
                        del section[level][idx]
                        return

        def _handle_rm_from_section(section, level, level_idx, scope):
            """
            :returns: True if level_idx is out of range and ignore_not_found is True else None
            """

            # rm by value
            if rm_value_mode:
                return _handle_rm_by_value(section, level, level_idx)

            if level_idx is not None:
                if not isinstance(section[level], list):
                    self._logger.warn(
                        "Key is not a list", key=level, at=".".join(scope)
                    )
                    raise RuntimeError("Key is not a list")

                # rm by index
                if isinstance(section[level], list):
                    self._logger.debug(
                        "Removing element from list", key=level, idx=level_idx
                    )
                    if ignore_not_found and len(section[level]) < level_idx:
                        self._logger.warn(
                            "Requested index to remove is out of range, ignoring.",
                            key=level,
                            idx=level_idx,
                        )
                        return True
                    del section[level][level_idx]
                    return

            # remove entire level
            self._logger.debug("Removing key", key=level)
            section.pop(level)

        def update_section(section, requested_levels, scope, section_idx=None):
            """
            Main recursion function, returns a mutated copy of section and a boolean indicating a change was made
            """
            self._logger.verbose(
                "update_section() called",
                section=section,
                requested_levels=requested_levels,
                scope=scope,
            )
            has_changed = False
            level = requested_levels[0]

            # Get index from level - if level is a list with a following index in square brackets
            level, level_idx = self._enrich_level_index(level)

            # idx only used if section is list
            subsection, idx = _get_subsection(section, level, section_idx)

            self._logger.verbose(
                "Got subsection",
                section=section,
                subsection=subsection,
                idx=idx,
                level=level,
                level_index=level_idx,
            )

            # reached final level, setting leaf value
            if len(requested_levels) == 1:

                # new dict key / list element. mutate section directly
                if subsection is None:
                    if not append_mode:
                        self._logger.warn(
                            "Key not found in dict", key=level, at=".".join(scope)
                        )
                        if rm_mode and ignore_not_found:
                            return section, False
                        raise RuntimeError("Key not found in dict")

                    new_value = value
                    if level_idx in [self.ListIndices.start, self.ListIndices.end, 0]:
                        new_value = helpers.as_list(value)
                    elif level_idx is not None:
                        self._logger.warn(
                            "List doesn't exist", key=level, at=".".join(scope)
                        )
                        raise RuntimeError("List doesn't exist")

                    self._logger.debug(
                        "Adding key",
                        key=level,
                        value=new_value,
                        at=".".join(scope),
                        section=section,
                        _type=section.__class__.__name__,
                    )

                    if isinstance(section, dict):
                        section[level] = new_value
                    elif isinstance(section, list):
                        section.append({level: new_value})

                    # section was some leaf (str/int)
                    else:
                        self._logger.debug(
                            "Coercing level to dict",
                            key=level,
                            at=".".join(scope),
                            section=section,
                            _type=section.__class__.__name__,
                        )
                        section = {str(section): {level: new_value}}
                    has_changed = True

                # subsection is not None, modifying existing leaf
                else:
                    ignored_missing_key = False
                    if rm_mode:
                        ignored_missing_key = _handle_rm_from_section(
                            section, level, level_idx, scope
                        )

                    else:

                        # subsection is the existing value
                        if subsection != value:
                            direct_insert = _handle_insert_in_section(
                                section, level, level_idx, scope
                            )
                            if direct_insert is not None:
                                subsection = direct_insert

                    # if ignored missing key, subsection was not changed and no need to rewrite it
                    # otherwise, a key was removed or inserted - assign the changed subsection and
                    # return the section was changed.
                    has_changed = not ignored_missing_key
                    if has_changed:
                        section = _assign_subsection(
                            section, level, subsection, section_idx
                        )

            # we need to go deeper
            else:

                # new dict for the requested_levels
                if subsection is None:
                    if not append_mode:
                        self._logger.warn(
                            "Subsection not found in dict",
                            key=level,
                            at=".".join(scope),
                        )
                        raise RuntimeError("Subsection not found in dict")

                    if isinstance(section, dict):
                        self._logger.debug(
                            "Creating subsection object",
                            object=level,
                            at=".".join(scope),
                        )
                        section.update({level: {}})
                        section[level], changed = update_section(
                            section[level], requested_levels[1:], scope + [level]
                        )
                    elif isinstance(section, list):
                        self._logger.debug(
                            "Creating sublist object", object=level, at=".".join(scope)
                        )
                        section.append({level: {}})
                        section[-1][level], changed = update_section(
                            section[-1][level], requested_levels[1:], scope + [level]
                        )

                    # section was a leaf in itself
                    else:
                        self._logger.debug(
                            "Creating subsection where a leaf once was",
                            section=section,
                            object=level,
                            at=".".join(scope),
                        )

                        key = str(section)
                        section = {key: {level: {}}}
                        section[key][level], changed = update_section(
                            section[key][level], requested_levels[1:], scope + [level]
                        )

                    has_changed = True

                # subsection not None, existing keys
                else:

                    # existing field is leaf and not subsection - handle separately for logging's sake only
                    if isinstance(subsection, int):
                        if not append_mode:
                            self._logger.warn(
                                "Leaf key was found where subsection expected (--add to overwrite)",
                                subsection=subsection,
                                at=section,
                            )
                            raise RuntimeError(
                                "Leaf was found where subsection expected"
                            )

                        else:
                            self._logger.debug(
                                "Overriding existing element with new subsection",
                                at=section,
                                subsection=subsection,
                                next_levels=requested_levels[1:],
                            )

                            subsection = {level: {}}
                            has_changed = True

                    # mutate subsection
                    subsection, changed = update_section(
                        subsection, requested_levels[1:], scope + [level], level_idx
                    )
                    has_changed |= changed

                    section = _assign_subsection(
                        section, level, subsection, section_idx
                    )

            return section, has_changed

        # split dict levels by '.' only, and clean up the escaping for the rest of the flow
        levels = [
            level.replace("\\.", ".") for level in re.split(r"(?<!\\)\.", full_key)
        ]
        return update_section(config, levels, [])

    def _merge_configs(self, config_a, config_b_filepath):
        self._logger.info("Merging with request file", file_path=config_b_filepath)
        config_b = self._load_config(config_b_filepath)
        config_a = self._deep_merge_dicts(config_a, config_b)
        return config_a, True

    def _deep_merge_dicts(self, d1, d2):
        """
        Recursively merges dict b into a [mutates a in-place].
        NOTE: overriding with b values if any conflicts occur, consistent with dict.update()
        """
        for key in d2:
            if key in d1:
                if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                    self._deep_merge_dicts(d1[key], d2[key])

                # same leaf value
                elif d1[key] == d2[key]:
                    pass

                # override a's value with b's value
                else:
                    d1[key] = d2[key]

            # adding b's value to the new dict
            else:
                d1[key] = d2[key]
        return d1
