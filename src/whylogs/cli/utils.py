import typing

import click

try:
    import colorama

    colorama.init()
except ImportError:
    pass


def echo(message: typing.Union[str, list], **styles):
    if isinstance(message, list):
        for msg in message:
            click.secho(msg, **styles)
    else:
        click.secho(message, **styles)
