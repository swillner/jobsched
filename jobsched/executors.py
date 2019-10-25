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

import subprocess
import sys
import time

from tqdm import tqdm

try:
    import pyslurm

    USE_PYSLURM = True
except ImportError:
    print("WARNING: Not using pyslurm")
    USE_PYSLURM = False


def tprint(s=""):
    tqdm.write(s, file=sys.stderr)


class Executor:
    def __init__(self):
        self.scheduled_count = 0


class DebugExecutor(Executor):
    def __init__(self):
        Executor.__init__(self)

    def close(self):
        tprint("Would have scheduled {} runs".format(self.scheduled_count))

    def init(self, name, cmd, workdir):
        tprint("Init {}".format(name))

    def open(self):
        pass

    def schedule(self, name, run_count, cmd, workdir, **kwargs):
        self.scheduled_count += run_count
        tprint("Schedule {}".format(name))
        return f'"{name}"'


class DryExecutor(Executor):
    def __init__(self):
        Executor.__init__(self)

    def close(self):
        tprint("Would have scheduled {} runs".format(self.scheduled_count))

    def init(self, name, cmd, workdir):
        tprint("\nInit {}".format(name))
        tprint(str(cmd))

    def open(self):
        pass

    def schedule(self, name, run_count, cmd, workdir, **kwargs):
        self.scheduled_count += run_count
        tprint("\nSchedule {}".format(name))
        tprint(cmd)
        return f'"{name}"'


class SlurmExecutor(Executor):
    def __init__(self, submission_delay):
        Executor.__init__(self)
        self.submission_delay = submission_delay
        self.progressbar = None

    def close(self):
        self.progressbar.close()

    def init(self, name, cmd, workdir):
        subprocess.check_output(cmd, shell=not isinstance(cmd, list), cwd=workdir)

    def open(self):
        self.progressbar = tqdm(unit="j", desc="Scheduling")

    def schedule(self, name, run_count, cmd, workdir, **kwargs):
        self.scheduled_count += run_count
        self.progressbar.update(run_count)

        if USE_PYSLURM:
            try:
                options = kwargs["pyslurm_options"]
                options["wrap"] = cmd
                run_id = pyslurm.job().submit_batch_job(options)
            except SystemExit as e:
                if e.code:
                    raise RuntimeError("Job submission failed")
            return str(run_id)

        while True:
            p = subprocess.Popen(
                ["sbatch", "--parsable"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                bufsize=4096,
            )
            p.stdin.write(bytes(cmd, "utf8"))
            p.stdin.close()
            run_id = p.stdout.read().decode("utf8").strip()
            if not p.wait():
                break
            input("Press enter...")
        time.sleep(self.submission_delay)
        return run_id


class LocalExecutor(Executor):
    def __init__(self):
        Executor.__init__(self)
        self.progressbar = None

    def close(self):
        self.progressbar.close()

    def init(self, name, cmd, workdir):
        subprocess.check_output(cmd, shell=not isinstance(cmd, list), cwd=workdir)

    def open(self):
        self.progressbar = tqdm(unit="j", desc="Running")

    def schedule(self, name, run_count, cmd, workdir, **kwargs):
        self.scheduled_count += run_count
        self.progressbar.update(run_count)
        proc = subprocess.Popen(
            ["bash", "-e"], stdin=subprocess.PIPE, bufsize=4096, cwd=workdir
        )
        proc.stdin.write(bytes(cmd, "utf8"))
        proc.stdin.close()
        if proc.wait():
            raise RuntimeError("job failed")
        return "local"
