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

import os


def deepupdate(base, new):
    if isinstance(base, dict):
        for k, v in base.items():
            if k in new:
                base[k] = deepupdate(v, new[k])
        for k, v in new.items():
            if not k in base:
                base[k] = v
    elif isinstance(base, list):
        for i, _ in enumerate(base):
            if i >= len(new):
                break
            base[i] = deepupdate(base[i], new[i])
        if len(new) > len(base):
            for i in range(len(base), len(new)):
                base.append(new[i])
    else:
        base = new
    return base


def ensure_abspath(path, relpath):
    if not path or path[0] != "/":
        return os.path.abspath(os.path.join(relpath, path))
    return os.path.abspath(path)


def get_setting(settings: dict, name: str, default=None):
    if name in settings:
        return settings[name]
    if default is not None:
        return default
    raise RuntimeError(f"Setting '{name}' required")
