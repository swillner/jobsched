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

import pystache

renderer = pystache.Renderer(escape=lambda u: u, missing_tags="strict")


class _RenderFilter(dict):
    def __init__(self, *dicts, first_missing_value="", repeated_missing_value=""):
        dict.__init__(self)
        for d in dicts:
            self.update(d)
        self.missing = set()
        self.first_missing_value = first_missing_value
        self.repeated_missing_value = repeated_missing_value

    def __getitem__(self, key):
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        if key in self.missing:
            return self.repeated_missing_value.format(key)
        if key[0] == "+":
            return eval(
                renderer.render(key[1:].replace("[[", "{{").replace("]]", "}}"), self)
            )
        self.missing.add(key)
        return self.first_missing_value.format(key)

    def __contains__(self, key):
        return True


class RenderError(RuntimeError):
    def __init__(self, template, missing, *dicts):
        message = "Error: missing parameters '{}' in template:\n".format(
            "', '".join(missing)
        )
        e = template.split("\n")
        MAX_LINES = 5
        message += "    " + "\n    ".join(e[:MAX_LINES])
        if len(e) > MAX_LINES:
            message += "\n    ..."
        message += "\n"
        message += "Parameters: {}\n".format(
            ", ".join(sorted(set(k for d in dicts for k in d.keys())))
        )
        RuntimeError.__init__(self, message)


def render(
    template,
    *dicts,
    output_missing=False,
    first_missing_value="",
    repeated_missing_value="",
):
    render_filter = _RenderFilter(
        *dicts,
        first_missing_value=first_missing_value,
        repeated_missing_value=repeated_missing_value,
    )
    text = renderer.render(template, render_filter)
    if output_missing:
        return text, render_filter.missing
    if render_filter.missing:
        raise RenderError(template, render_filter.missing, *dicts)
    return text
