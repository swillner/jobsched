#!/usr/bin/env python3

# Copyright (C) Sven Willner <sven.willner@pik-potsdam.de>
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

import sys

try:
    import termios
    import tty
    from blessings import Terminal

    can_use_pick = True
except ImportError:
    can_use_pick = False


def getch():
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch == "\x1b":
            ch += sys.stdin.read(2)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def pick(captions, values=None, title=None, stream=None, fullscreen=False):
    if stream is None:
        stream = sys.stderr
    term = Terminal(stream=stream)
    stream.write(term.hide_cursor)
    if fullscreen:
        stream.write(term.enter_fullscreen)
        stream.write(term.move(0, 0))
    stream.flush()
    with term.location():
        try:
            cur = 0
            if title is not None:
                stream.write("%s\n" % title)
            stream.write("{t.reverse} {s} {t.normal}".format(t=term, s=captions[0]))
            for c in captions[1:]:
                stream.write("\n{t.save} {s} ".format(t=term, s=c))
            stream.write("{t.restore}".format(t=term))
            for c in captions[1:]:
                stream.write("{t.move_up}".format(t=term))
            stream.write("{t.save}".format(t=term))
            while True:
                stream.flush()
                k = getch()
                if k == "\x1b[A":  # up
                    if cur > 0:
                        stream.write(
                            " {sa} {t.restore}{t.move_up}{t.save}{t.reverse} {sb} {t.normal}{t.restore}".format(
                                t=term, sa=captions[cur], sb=captions[cur - 1]
                            )
                        )
                        cur -= 1
                elif k == "\x1b[B":  # down
                    if cur < len(captions) - 1:
                        stream.write(
                            " {sa} {t.restore}{t.move_down}{t.save}{t.reverse} {sb} {t.normal}{t.restore}".format(
                                t=term, sa=captions[cur], sb=captions[cur + 1]
                            )
                        )
                        cur += 1
                elif k == "\r":
                    if values is not None:
                        return values[cur]
                    return cur
                elif k == "\x03":
                    raise RuntimeError("Aborted")
        finally:
            stream.write(term.normal_cursor)
            if fullscreen:
                stream.write(term.exit_fullscreen)
            else:
                stream.write(term.clear_eos)
            stream.flush()
