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
#!/usr/bin/env python

import os
import sys
import traceback
import argparse

from twisted.internet import defer, reactor

import clients.logging
import core


def enrich_args(run_args):
    """
    Make all paths absolute
    """
    if run_args.config:
        run_args.config = os.path.abspath(run_args.config)

    return run_args


def _run(run_args):
    retval = 1

    def _on_exception(failure):
        try:

            # adding an error will set logger.first_error if it's not already set
            logger.error("Relayer failed with exception", exc=str(failure))
        except Exception as exc2:
            traceback.print_exc()
            print("Relayer failed, and error logging failed. exc={0}".format(exc2))
        finally:
            reactor.callFromThread(reactor.stop)

    try:
        logger = clients.logging.Client(
            "relayer",
            initial_severity=clients.logging.Severity.Info,
            output_stdout=not run_args.log_disable_stdout,
            output_dir=run_args.log_output_dir,
            max_log_size_mb=run_args.log_file_rotate_max_file_size,
            max_num_log_files=run_args.log_file_rotate_num_files,
            log_file_name=run_args.log_file_name,
            log_colors=run_args.log_colors,
        ).logger
        rlr = core.Relayer(logger, run_args.config, run_args.debug)

        d = defer.maybeDeferred(
            rlr.relayer_config,
            run_args.add,
            run_args.rm,
            run_args.update,
            run_args.extend_list,
            run_args.insert,
            run_args.rm_list_element,
            run_args.from_file,
            run_args.ignore_not_found,
        )

        # after run
        # d.addBoth(lambda _: reactor.callFromThread(reactor.stop))
        d.addCallbacks(lambda _: reactor.callFromThread(reactor.stop), _on_exception)

        reactor.run()

        if logger.first_error is None:
            retval = 0

    except Exception as exc:
        _on_exception(exc)

    return retval


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Relayer")

    # add logging args
    clients.logging.Client.register_arguments(parser)

    parser.add_argument(
        "-c", "--config", help="Path to the config file (yml/json format).", type=str
    )

    parser.add_argument(
        "-d",
        "--debug",
        help="Dumps the modified config to stdout instead of rewriting the file.",
        action="store_true",
    )

    parser.add_argument(
        "-a",
        "--add",
        help="One or more of a.b.c=X type k-v args to add to config (or replace if exists). "
        "For Lists - use the syntax a.b.c[idx]=X or a.b.c[start|end]=X to insert single value. "
        "Value can be a primitive, a list (comma separated, no whitespaces) or a list of "
        "dictionaries (format: \\{aa:bb\\},\\{cc:dd,kk:ll\\}), no whitespaces. If adding a list "
        "to a list - it will add a list as a value. For extending use --extend",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-i",
        "--insert",
        help="One or more of a.b.c[idx]=X type k-v args to add to config. "
        "Value can be a primitive, a list (comma separated, no whitespaces) or a list "
        "of dictionaries (format: \\{aa:bb\\},\\{cc:dd,kk:ll\\}), no whitespaces.",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-r",
        "--rm",
        help="One or more of a.b.c type k-v args to remove from config. "
        "For Lists - use the syntax a.b.c[idx]. "
        "Value can be a primitive, a list (comma separated, no whitespaces) or a list "
        "of dictionaries (format: \\{aa:bb\\},\\{cc:dd,kk:ll\\}, no whitespaces).",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-u",
        "--update",
        help="One or more of a.b.c=X type k-v args to update in config. "
        "Value can be a primitive, a list (comma separated, no whitespaces) or a list "
        "of dictionaries (format: \\{aa:bb\\},\\{cc:dd,kk:ll\\}, no whitespaces).",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-e",
        "--extend-list",
        help="One or more of a.b.c[]=d,e or a.b.c[start]=d,e type k-v args to update in config. "
        "Value can be a primitive, a list (comma separated, no whitespaces) or a list "
        "of dictionaries (format: \\{aa:bb\\},\\{cc:dd,kk:ll\\}, no whitespaces).",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-rl",
        "--rm-list-element",
        help="One or more of a.b.c[X] args to remove from config. "
        "Value can be a primitive or json representation of a dictionary.",
        action="append",
        type=str,
    )

    parser.add_argument(
        "-ff",
        "--from-file",
        help="This will read the given file, assuming yaml compatible format, and merge this "
        "into the target config.",
        type=str,
    )

    parser.add_argument(
        "-inf",
        "--ignore-not-found",
        help="When removing entries, don't fail if entry doesn't exist.",
        action="store_true",
    )

    args = parser.parse_args()
    retval = _run(enrich_args(args))

    # return value
    sys.exit(retval)
