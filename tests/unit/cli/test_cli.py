from click.testing import CliRunner
import whylogs.cli as client
from whylogs.cli.demo_cli import cli as democli


def test_init_cmd():
  runner = CliRunner()
  result = runner.invoke(client.cli, ['init'])
  print(result.output)
  assert result.exit_code == 0


def test_demo_init():
  runner = CliRunner()
  result = runner.invoke(democli, ['init'])
  print(result.output)
  assert result.exit_code == 0
