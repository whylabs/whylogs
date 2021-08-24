import logging

import click

from whylogs import __version__ as whylogs_version
from whylogs.cli.init import init

try:
    import colorama

    colorama.init()
except ImportError:
    pass


def _set_up_logger():
    # Log to console with a simple formatter; used by CLI
    formatter = logging.Formatter("%(message)s")
    handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    module_logger = logging.getLogger("whylogs")
    module_logger.addHandler(handler)
    module_logger.setLevel(level=logging.WARNING)

    return module_logger


@click.group()
@click.version_option(version=whylogs_version)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set whylogs CLI to use verbose output.",
)
def cli(verbose):
    """
    Welcome to whylogs CLI!

    Supported basic commands:

    - whylogs init : create a new whylogs project configuration
    """
    logger = _set_up_logger()
    if verbose:
        logger.setLevel(logging.DEBUG)


cli.add_command(init)


def main():
    _set_up_logger()
    cli()


if __name__ == "__main__":
    main()
