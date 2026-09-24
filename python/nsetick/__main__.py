"""The ``nsetick`` command, installed with the Python package.

The same command line as the standalone binary in the release archives: it runs the same
Rust code, compiled into the package. ``nsetick --help`` after ``pip install nsetick``, or
``python -m nsetick --help``.
"""

import signal
import sys


def main() -> int:
    # Behave like a native command line tool. Python turns Ctrl-C into KeyboardInterrupt, which
    # only fires once control returns to Python - never, during a long parse - so restore the
    # default: Ctrl-C ends the process at once. Likewise for a closed pipe (`| head`).
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    from nsetick._native import cli

    argv = sys.argv[:]
    argv[0] = "nsetick"
    return cli(argv)


if __name__ == "__main__":
    sys.exit(main())
