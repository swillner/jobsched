#!/usr/bin/env python3

# Copyright (C) 2016-2019 Sven Willner <sven.willner@pik-potsdam.de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import os
import shelve
import subprocess
import sys

import pyaml
from ruamel import yaml
from tqdm import tqdm

from .executors import DebugExecutor, DryExecutor, LocalExecutor, SlurmExecutor, tprint
from .helpers import ensure_abspath, get_setting
from .jobs import JobList
from .parameters import ParameterCombinations
from .utils import can_use_pick, pick


def load_runfile(filename):
    if filename.endswith(".yml"):
        if os.path.exists(filename):
            f = open(filename, "r")
            former_runs = yaml.round_trip_load(f)
        else:
            f = open(filename, "w")
            former_runs = {}
    else:
        f = shelve.open(filename)
        former_runs = f.get("former_runs", {})
    return f, former_runs


def command_run(settings):
    parser = argparse.ArgumentParser(description="schedule runs for job")
    parser.add_argument(
        "--dry", action="store_true", help="dry run, do not actually schedule jobs"
    )
    parser.add_argument(
        "--local", action="store_true", help="do not schedule, but run locally"
    )
    parser.add_argument(
        "--debug", action="store_true", help="only show which jobs would be scheduled"
    )
    parser.add_argument(
        "--force", action="store_true", help="force rescheduling of given job"
    )
    parser.add_argument(
        "--workdir", type=str, default="out", help="working directory (default: out)"
    )
    parser.add_argument(
        "--logdir", type=str, default="log", help="log directory (default: log)"
    )
    parser.add_argument(
        "--submission-delay",
        type=float,
        default=0.1,
        help="delay between submissions in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--settings",
        type=str,
        default="{}",
        help="settings to overwrite from jobs file",
    )
    parser.add_argument(
        "--runfile",
        type=str,
        default="jobs.run",
        help="file to read/write scheduled runs from/to",
    )
    parser.add_argument("job", type=str, nargs="?", help="name of job")
    args = parser.parse_args(sys.argv[2:])

    args.workdir = os.path.abspath(args.workdir)
    args.logdir = os.path.abspath(args.logdir)

    if args.job is None:
        js = sorted(settings["jobs"].keys())
        if len(js) == 1:
            job = js[0]
        elif can_use_pick:
            job = pick(js, js, "Job:")
        else:
            raise RuntimeError("Please specify a job")
    else:
        job = args.job

    runfile, former_runs = load_runfile(args.runfile)
    if not args.dry and not args.debug:
        if not os.path.exists(args.logdir):
            os.mkdir(args.logdir)

    settings.update(
        yaml.round_trip_load(args.settings) if args.settings is not None else {}
    )
    if not "account" in settings:
        if args.debug:
            settings["account"] = "account"
        else:
            settings["account"] = (
                subprocess.check_output(["slurm-bestaccount"], shell=True)
                .decode("utf8")
                .strip()
            )
    settings["logdir"] = args.logdir
    settings["workdir"] = args.workdir

    if args.debug:
        executor = DebugExecutor()
    elif args.dry:
        executor = DryExecutor()
    elif args.local:
        executor = LocalExecutor()
    else:
        executor = SlurmExecutor(args.submission_delay)

    progressbar = tqdm(unit="j", desc="Preparing", leave=False)
    joblist = JobList(settings, former_runs, executor)
    possible = ParameterCombinations(get_setting(settings, "foreach"))
    run_job = joblist.get_job(job, possible)
    progressbar.close()

    executor.open()
    run_job.schedule_tree(possible, {}, args.force)
    executor.close()

    if not args.debug and not args.dry and not args.local:
        if args.runfile:
            if args.runfile.endswith(".yml"):
                runfile.write(pyaml.dump(former_runs))
            else:
                runfile["former_runs"] = former_runs
            runfile.close()


def command_runid(settings):
    parser = argparse.ArgumentParser(description="show job corresponding to runid")
    parser.add_argument(
        "--runfile",
        type=str,
        default="jobs.run",
        help="file to read/write scheduled runs from/to",
    )
    parser.add_argument("runid", type=str, help="id of run")
    args = parser.parse_args(sys.argv[2:])

    _, former_runs = load_runfile(args.runfile)
    for jobname, runs in former_runs.items():
        for params, info in runs.items():
            if str(info["id"]).startswith(args.runid):
                tprint("{}: {}({}) {}".format(info["id"], jobname, params, info))


def command_tree(settings):
    import asciitree

    # parser = argparse.ArgumentParser(description="print dependency tree")
    # args = parser.parse_args(sys.argv[2:])
    tree = {j: True for j in settings["jobs"].keys()}
    for j in settings["jobs"].values():
        for d in j.get("depends", []):
            if d["job"] in tree:
                del tree[d["job"]]

    def addjob(js):
        return {
            j["job"]: addjob(settings["jobs"][j["job"]].get("depends", [])) for j in js
        }

    tr = asciitree.LeftAligned(
        draw=asciitree.drawing.BoxStyle(gfx=asciitree.drawing.BOX_LIGHT, horiz_len=1)
    )
    for j in sorted(tree.keys()):
        tprint(tr(dict([(j, addjob(settings["jobs"][j].get("depends", [])))])))
        tprint()


def main():
    parser = argparse.ArgumentParser(
        description="Schedules runs for a dependencies tree of jobs for given parameter combinations/files",
        usage="jobsched <command> [<args>]\n\n"
        "Commands:\n"
        "    log    Show job log\n"
        "    run    Run job\n"
        "    runid  Show runids of job\n"
        "    status Show job statuses\n"
        "    tree   Print dependency tree\n",
        epilog="Written by Sven Willner <sven.willner@pik-potsdam.de>",
    )
    parser.add_argument("command", type=str, help="job scheduler command")
    args = parser.parse_args(sys.argv[1:2])

    with open("jobs.yml", "r") as f:
        settings = yaml.round_trip_load(f.read())

    if "const" not in settings:
        settings["const"] = {}
    settings["const"]["_scriptsdir"] = os.path.abspath("scripts")

    if "foreach" not in settings:
        settings["foreach"] = {}
    for f in settings["foreach"]:
        if not isinstance(settings["foreach"][f], list):
            settings["foreach"][f] = eval(str(settings["foreach"][f]))
            if isinstance(settings["foreach"][f], range):
                settings["foreach"][f] = list(settings["foreach"][f])
            elif not isinstance(settings["foreach"][f], list):
                settings["foreach"][f] = [settings["foreach"][f]]

    COMMANDS = {"run": command_run, "runid": command_runid, "tree": command_tree}
    if args.command not in COMMANDS:
        raise RuntimeError("Command {} not found".format(args.command))
    COMMANDS[args.command](settings)
