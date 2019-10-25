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

import hashlib
import itertools
import os
import re
import subprocess
from copy import deepcopy

from ruamel import yaml

from .executors import Executor
from .helpers import deepupdate, ensure_abspath, get_setting
from .parameters import ParameterCombinations, ParameterValues
from .templates import render

try:
    import pyslurm

    USE_PYSLURM = True
except ImportError:
    print("WARNING: Not using pyslurm")
    USE_PYSLURM = False

VALID_JOB_PROPERTIES = [
    "code",
    "depends",
    "epilog",
    "filename",
    "foreach",
    "init",
    "output",
    "parameters",
    "prolog",
    "scheduler",
    "settings",
    "time",
    "workdir",
]
VALID_SETTINGS = [
    "account",
    "const",
    "foreach",
    "jobs",
    "logdir",
    "provenance_variables",
    "scheduler",
    "skip_in_name",
    "workdir",
]
SPECIAL_PARAM_PREFIX = "_"


class JobState:
    DONE = 0
    WAITING = 1
    FAILED = 2
    RUNNING = 3


SLURM_JOB_STATE_IDS = {
    0: JobState.WAITING,  # JOB_PENDING
    1: JobState.RUNNING,  # JOB_RUNNING
    2: JobState.WAITING,  # JOB_SUSPENDED
    3: JobState.DONE,  # JOB_COMPLETE
    4: JobState.FAILED,  # JOB_CANCELLED
    5: JobState.FAILED,  # JOB_FAILED
    6: JobState.FAILED,  # JOB_TIMEOUT
    7: JobState.FAILED,  # JOB_NODE_FAIL
    8: JobState.WAITING,  # JOB_PREEMPTED
    10: JobState.FAILED,  # JOB_BOOT_FAIL
}

def get_run_state(run_id: str):
    run_id = run_id.strip()
    if run_id == "local":
        return JobState.DONE

    if USE_PYSLURM:
        res = pyslurm.slurmdb_jobs().get(jobids=[run_id])[int(run_id)]["state"]
        return SLURM_JOB_STATE_IDS[res]

    while True:
        p = subprocess.Popen(
            ["sacct", "-j", str(run_id), "-nP", "-o", "state"], stdout=subprocess.PIPE
        )
        res = p.stdout.read().decode("utf8").split("\n")[0].strip()
        if not p.wait():
            break
        input("Press enter...")
    if not res:  # waiting array
        return JobState.WAITING
    if res == "PENDING":
        return JobState.WAITING
    if res == "RUNNING":
        return JobState.RUNNING
    if (
        res == "FAILED"
        or res == "CANCELLED"
        or res == "TIMEOUT"
        or res == "OUT_OF_MEMORY"
    ):
        return JobState.FAILED
    if res == "COMPLETED":
        return JobState.DONE
    raise RuntimeError(f"Unknown job state '{res}'")


def to_minutes(time_str: str):
    m = re.match("([0-9]+)-([0-9][0-9]):([0-9][0-9]):([0-9][0-9])", time_str)
    if m:
        days = int(m.group(1))
        hours = int(m.group(2))
        minutes = int(m.group(3))
        seconds = int(m.group(4))
        return (1 if seconds > 0 else 0) + minutes + hours * 60 + days * 24 * 60
    m = re.match("([0-9]?[0-9]):([0-9][0-9]):([0-9][0-9])", time_str)
    if m:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        seconds = int(m.group(3))
        return (1 if seconds > 0 else 0) + minutes + hours * 60
    m = re.match("([0-9]?[0-9]):([0-9][0-9])", time_str)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        return (1 if seconds > 0 else 0) + minutes
    raise RuntimeError(f"Invalid time format '{time_str}'")


def run_description(current, ignore=None):
    if ignore is None:
        ignore = []

    def shorten(s):
        return "".join([a[0] if a else "" for a in s.split("_")])

    res = ""
    for n, p in sorted(current.items(), key=lambda n: n[0]):
        if n not in ignore:
            res += "_{}{}".format(
                shorten(n), p if not isinstance(p, bool) else 1 if p else 0
            )
    return res


class JobList:
    def __init__(self, settings: dict, former_runs: dict, executor: Executor):
        for k in settings:
            if not k in VALID_SETTINGS:
                raise RuntimeError(f"Unknown setting '{k}'")
        self.settings = deepcopy(settings)
        self.constants = get_setting(self.settings, "const", {})
        self.executor = executor
        self.former_runs = former_runs
        self.jobdescs = get_setting(self.settings, "jobs")

    def get_jobdesc(self, jobname: str):
        if not jobname in self.jobdescs:
            raise RuntimeError(f"Unknown job '{jobname}'")
        return self.jobdescs[jobname]

    def get_job(self, jobname: str, combinations: ParameterCombinations):
        self._add_inheritance(jobname)
        self._add_filecombinations(jobname, combinations, set([]))
        return self._setup_job(jobname)

    def _add_inheritance(self, jobname: str):
        jobdesc = self.get_jobdesc(jobname)
        if "inherits" in jobdesc:
            parent = self._add_inheritance(jobdesc["inherits"])
            for k, v in deepcopy(parent).items():
                if k in jobdesc:
                    jobdesc[k] = deepupdate(v, jobdesc[k])
                else:
                    jobdesc[k] = v
            del jobdesc["inherits"]
        for s in jobdesc.get("depends", []):
            self._add_inheritance(s["job"])
        return jobdesc

    def _add_filecombinations(
        self, jobname: str, combinations: ParameterCombinations, seen: set
    ):
        jobdesc = self.get_jobdesc(jobname)
        for s in jobdesc.get("depends", []):
            self._add_filecombinations(s["job"], combinations, seen)

        if not jobname in seen:
            for filepattern in jobdesc.get("foreach", []):
                combinations.add_filecombinations(
                    filepattern,
                    get_setting(self.settings, "workdir"),
                    self.constants,
                    jobdesc.get("parameters", {}),
                )
        seen.add(jobname)

    def _setup_job(self, jobname: str):
        jobdesc = self.get_jobdesc(jobname)
        dependencies = [
            (self._setup_job(s["job"]), s["foreach"])
            for s in jobdesc.get("depends", [])
        ]
        if jobname in self.former_runs:
            former_runs = self.former_runs[jobname]
        else:
            former_runs = {}
            self.former_runs[jobname] = former_runs
        return Job(
            jobname, jobdesc, dependencies, self.settings, former_runs, self.executor
        )


class Job:
    def __init__(
        self,
        jobname: str,
        jobdesc: dict,
        dependencies,  #: list[tuple[Job, list[str]]],
        settings: dict,
        former_runs: dict,
        executor: Executor,
    ):

        for k in jobdesc:
            if not k in VALID_JOB_PROPERTIES:
                raise RuntimeError(f"Unknown property '{k}' for '{jobname}'")

        self.executor = executor
        self.name = jobname

        if "filename" in jobdesc:
            if jobdesc["filename"].endswith(".sh"):
                self.codetype = "shell"
            elif jobdesc["filename"].endswith(".py") or jobdesc["filename"].endswith(
                ".py3"
            ):
                self.codetype = "python"
            else:
                raise RuntimeError(
                    "Unknown file extension for {}".format(jobdesc["filename"])
                )
            with open("scripts/{}".format(jobdesc["filename"]), "r") as f:
                self.code = f.read()
        elif "code" in jobdesc:
            self.code = jobdesc["code"]
            self.codetype = "shell"
        else:  # Dummy job
            self.code = ""
            self.codetype = "shell"
            self.time = "0:00"

        self.variables = set([])
        self.parameters = jobdesc.get("parameters", {})
        self.parameters.update(get_setting(settings, "const", {}))

        if "settings" in jobdesc:
            self.parameters["settings"] = yaml.round_trip_dump(jobdesc["settings"])

        for filepattern in jobdesc.get("foreach", []):
            _, missing = render(filepattern, self.parameters, output_missing=True)
            self.variables.update(k for k in missing if k[0] != SPECIAL_PARAM_PREFIX)

        _, missing = render(
            "\n".join(str(v) for v in self.parameters.values()),
            self.parameters,
            output_missing=True,
        )
        self.variables.update(k for k in missing if k[0] != SPECIAL_PARAM_PREFIX)

        if not "_provenance_ncatted" in self.parameters:
            pro = "ncatted -h -O -a history,global,d,,"
            for k, v in get_setting(settings, "provenance_variables", {}).items():
                _, missing = render(v, self.parameters, output_missing=True)
                if not missing - self.variables:  # no variable unused by this job
                    pro += f' -a {k},global,o,c,"{v}"'
            self.parameters["_provenance_ncatted"] = pro

        self.array = jobdesc.get("array", False)
        self.workdir = jobdesc.get("workdir", "")
        self.prolog = jobdesc.get("prolog", "")
        self.epilog = jobdesc.get("epilog", "")
        self.foreach = jobdesc.get("foreach", [])
        self.output = jobdesc.get("output", [])
        self.init = jobdesc.get("init", [])
        self.scheduler = deepcopy(get_setting(settings, "scheduler", {}))
        self.scheduler.update(jobdesc.get("scheduler", {}))

        self.parameters["_threads"] = self.scheduler.get("threads", 1)

        self.dependencies = dependencies
        self.former_runs = former_runs
        self.scheduled_runs = {}
        self.settings = settings

    def init_run(self, current, parameters, workdir):
        """Initializes a particular run of a job"""
        name = "{}({})".format(self.name, current)
        for i in self.init:
            if "code" in i:
                cmd = i["code"]
            else:
                with open("scripts/{}".format(i["filename"]), "r") as f:
                    cmd = f.read()
            self.executor.init(
                name, render(cmd, self.parameters, current, parameters), workdir
            )

    def schedule_run(self, current, parameters, dep_run_ids, workdir):
        """Schedules a particular run of a job"""

        if dep_run_ids is None:
            dep_run_ids = ""
        else:
            dep_run_ids = ":".join(
                sorted(
                    set(
                        run_id.split("_")[0].strip()
                        for run_id in dep_run_ids
                        if run_id != "local"
                    )
                )
            )

        template_parameters = {}
        array_cmd = ""

        if self.array:
            if self.codetype != "shell":
                raise RuntimeError("Arrays only supported for shell jobs")
            p = set(current[0].items())
            for p_ in current[1:]:
                p = set(p & set(p_.items()))
            name = "{}(len: {}, {})".format(
                self.name, len(current), ParameterValues(dict(p))
            )
            output = "{}/%A-%a".format(get_setting(self.settings, "logdir"))
            array_str = "0-{}".format(len(parameters) - 1)
            # if "array_size" in self.settings:
            #     array_str += "%{}".format(self.settings["array_size"])
            parameter_names = {}
            for p in [parameters, current]:
                if p:
                    for n in p[0]:
                        parameter_names[
                            n
                        ] = "${{PARAM_{}[$SLURM_ARRAY_TASK_ID]}}".format(n)
                    for i, nv in enumerate(p):
                        for n, v in nv.items():
                            array_cmd += f"export PARAM_{n}[{i}]='{v}'\n"
            template_parameters.update(self.parameters)
            template_parameters.update(parameter_names)
        else:
            name = "{}({})".format(self.name, current)
            output = "{}/%j".format(get_setting(self.settings, "logdir"))
            array_str = ""
            template_parameters.update(self.parameters)
            template_parameters.update(current)
            template_parameters.update(parameters)

        slurm_options = {
            "account": get_setting(self.settings, "account"),
            "acctg-freq": "energy=0",
            "array": array_str,
            "constraint": self.scheduler.get("constraint", ""),  # e.g. broadwell
            "cpus-per-task": self.scheduler.get("threads", 1),
            "error": output,
            "export": "ALL",
            "job-name": name,
            "kill-on-invalid-dep": "yes",
            "mail-type": self.scheduler.get("notify", "NONE"),
            "nice": 0,
            "output": output,
            "partition": self.scheduler.get("partition", "standard"),
            "profile": "none",
            "qos": self.scheduler.get("qos", "short"),
            "time": self.scheduler.get("time", "1-00:00:00"),
            "workdir": workdir,
        }

        pyslurm_options = {
            "account": slurm_options["account"],
            "acctg_freq": slurm_options["acctg-freq"],
            "array_inx": slurm_options["array"],
            "constraints": slurm_options["constraint"],
            "dependency": "afterok:{}".format(dep_run_ids) if dep_run_ids else "",
            "error": slurm_options["error"],
            # "export_env": slurm_options["export"],
            "job_flags": 1,  # KILL_INV_DEP
            "job_name": slurm_options["job-name"],
            "cpus_per_task": int(slurm_options["cpus-per-task"]),
            "nice": int(slurm_options["nice"]),
            "output": slurm_options["output"],
            "partition": slurm_options["partition"],
            "profile": 1,  # ACCT_GATHER_PROFILE_NONE
            "qos": slurm_options["qos"],
            "time_limit": int(to_minutes(slurm_options["time"])),
            # TODO "hold": True,
            # TODO "mail_type": slurm_options["mail-type"],
        }

        slurm_header = (
            "\n".join(
                "#SBATCH --{}='{}'".format(k, v)
                for k, v in slurm_options.items()
                if str(v)
            )
            + "\n"
        )

        template_parameters["_slurm_header"] = slurm_header
        template_parameters["_workdir"] = workdir

        cmd = """\
#!/bin/bash
{slurm_header}\
{dependencies}\
{array}\
echo "STARTING {name} @ $(date +'%FT%T')"

export OMP_PROC_BIND=FALSE
export OMP_NUM_THREADS={threads}
cd "{workdir}"

ret=$?
if [[ $ret == 0 ]]
then
    bash -e <<'PROLOG'
{prolog}
PROLOG
    ret=$?
fi
if [[ $ret == 0 ]]
then
    {interpreter} <<'{hash}'
{code}
{hash}
    ret=$?
fi
if [[ $ret == 0 ]]
then
    bash -e <<'EPILOG'
{epilog}
EPILOG
    ret=$?
fi
if [[ $ret == 0 ]]
then
    echo "DONE {name} @ $(date +'%FT%T')"
else
    echo "FAILED {name} @ $(date +'%FT%T')"
fi
exit $ret
""".format(
            **{
                "array": array_cmd,
                "code": self.code,
                "dependencies": "#SBATCH --depend='afterok:{}'\n".format(dep_run_ids)
                if dep_run_ids
                else "",
                "epilog": self.epilog,
                "hash": hashlib.sha1(self.code.encode()).hexdigest(),
                "interpreter": {"shell": "bash -e", "python": "python3"}[self.codetype],
                "name": name,
                "prolog": self.prolog,
                "slurm_header": slurm_header,
                "threads": slurm_options["cpus-per-task"],
                "workdir": workdir,
            }
        )

        cmd = render(cmd, template_parameters)

        run_id = self.executor.schedule(
            name,
            len(parameters) if self.array else 1,
            cmd,
            workdir,
            pyslurm_options=pyslurm_options,
        )
        return run_id

    def schedule_tree(self, possible, current, forcestart=False):
        """Schedule dependencies and then job"""
        current = dict(item for item in current.items() if item[0] in self.variables)
        run_ids = []
        combinations = possible.recombine(current, list(self.variables - set(current)))
        if self.array:
            all_combinations = []
            all_parameters = []
            all_dependencies = []

        for c in combinations:
            if (
                c in self.former_runs
                and not c in self.scheduled_runs
                and not self.former_runs[c].get("success", False)
            ):
                state = get_run_state(self.former_runs[c]["id"])
                if state == JobState.DONE:
                    self.former_runs[c]["success"] = True
            else:
                state = JobState.DONE

            parameters = {
                f"_p{i}": v
                for i, v in enumerate(
                    render(filepattern, c, self.parameters)
                    for filepattern in self.foreach
                )
            }
            parameters["_desc"] = "{}({})".format(self.name, c)
            parameters["_longname"] = "{}{}".format(
                self.name,
                run_description(c, ignore=self.settings.get("skip_in_name", [])),
            )
            outputfiles = {
                f"_output{i}": v
                for i, v in enumerate(
                    render(filepattern, c, self.parameters, parameters)
                    for filepattern in self.output
                )
            }

            workdir = ensure_abspath(
                render(self.workdir, c, self.parameters),
                get_setting(self.settings, "workdir"),
            )
            if not os.path.exists(workdir):
                self.executor.init(self.name, ["mkdir", "-p", workdir], ".")

            # output_missing = False
            # for f in outputfiles.values():
            #     if os.path.exists(os.path.join(workdir, f)):
            #         if state not in [
            #             JobState.FAILED,
            #             JobState.RUNNING,
            #             JobState.WAITING,
            #             JobState.DONE,
            #         ] and (
            #             not c in self.former_runs
            #             or not self.former_runs[c].get("success", False)
            #         ):
            #             tprint(f"Warning: File {f} already exists")
            #     else:
            #         output_missing = True

            parameters.update(outputfiles)

            # if not (state == JobState.DONE and c in self.former_runs) and not output_missing and not forcestart:
            #    tprint("Output present for {}".format(parameters["_desc"]))
            #    if not c in self.former_runs:
            #        self.former_runs[c] = {}
            #    self.former_runs[c]["success"] = True
            #    continue

            # still_running = c in self.scheduled_runs or state in [JobState.RUNNING, JobState.WAITING]
            has_failed = state == JobState.FAILED
            already_scheduled = c in self.former_runs or c in self.scheduled_runs

            if has_failed or forcestart or not already_scheduled:
                if state != JobState.FAILED:
                    self.init_run(c, parameters, workdir)
                dep_run_ids = []
                for dep, foreach in self.dependencies:
                    dep_run_ids += dep.schedule_tree(
                        possible, {k: v for k, v in c.items() if k in foreach}
                    )
                if self.array:
                    all_combinations.append(c)
                    all_parameters.append(parameters)
                    all_dependencies.append(dep_run_ids)
                else:
                    run_id = self.schedule_run(c, parameters, dep_run_ids, workdir)
                    run_ids.append(run_id)
                    self.former_runs[c] = {"id": run_id, "success": False}
                    self.scheduled_runs[c] = run_id
            else:
                run_ids.append(self.former_runs[c]["id"])

        if self.array and all_combinations:
            run_id = self.schedule_run(
                all_combinations,
                all_parameters,
                set(itertools.chain(*all_dependencies)),
                workdir,
            )
            for i, c in enumerate(all_combinations):
                array_run_id = f"{run_id}_{i}"
                self.former_runs[c] = {"id": array_run_id, "success": False}
                self.scheduled_runs[c] = array_run_id
            run_ids.append(run_id)
        return run_ids
