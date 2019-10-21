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

import re
import os
from glob import glob

from .templates import render


def dictunion(a, b):
    tmp = a.copy()
    tmp.update(b)
    return tmp


class ParameterValues(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))

    def __str__(self):
        return ", ".join((f"{key}: {value}" for key, value in sorted(self.items())))


class ParameterCombinations:
    def __init__(self, combinations):
        if isinstance(combinations, dict):
            self.combinations = [{}]
            for k, v in combinations.items():
                if isinstance(v[0], dict):
                    self.combinations = [
                        dictunion(a, b) for a in self.combinations for b in v
                    ]
                else:
                    self.combinations = [
                        dictunion(a, {k: b}) for a in self.combinations for b in v
                    ]
        else:
            self.combinations = list(combinations).copy()

    def add_filecombinations(self, filepattern, workdir, *dicts):
        new = []
        for v in self.combinations:
            files, missing = render(
                filepattern,
                v,
                *dicts,
                output_missing=True,
                first_missing_value="*",
                repeated_missing_value="*",
            )
            if missing:
                filepattern_regexp, _ = render(
                    filepattern,
                    v,
                    *dicts,
                    output_missing=True,
                    first_missing_value="(?P<{}>[^/]*)",
                    repeated_missing_value="(?P=<{}>[^/]*)",  # backreference
                )
                if files[0] != "/":
                    files = os.path.join(workdir, files)
                if filepattern_regexp[0] != "/":
                    filepattern_regexp = os.path.join(workdir, filepattern_regexp)
                r = re.compile(
                    filepattern_regexp.replace(".", r"\.")
                    .replace("*", r"[^\/]*")
                    .replace("+", r"\+")
                )
                for f in glob(files):
                    n = {}
                    for m in missing:
                        fmatch = r.match(f)
                        if fmatch is None:
                            raise RuntimeError(f"{f} does not match {r}")
                        n[m] = fmatch.group(m)
                    n.update(v)
                    new.append(n)
            else:
                new.append(v)
        self.combinations = new

    def recombine(self, values, freekeys):
        return frozenset(
            ParameterValues({key: v1[key] for key in list(values.keys()) + freekeys})
            for v1 in (
                v2
                for v2 in self.combinations
                if not any(i for i in values.items() if i[1] != v2[i[0]])
            )
        )

    def __repr__(self):
        return repr(self.combinations)
