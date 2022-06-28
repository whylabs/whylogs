import os
import subprocess

import nbformat
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.join(TEST_DIR, os.pardir, os.pardir)
skip_notebooks = ["Mlflow_Logging.ipynb", "Pyspark_Profiling.ipynb", "Writing_Profiles.ipynb", "Kafka_Example.ipynb"]


# https://docs.pytest.org/en/6.2.x/example/parametrize.html#a-quick-port-of-testscenarios
def pytest_generate_tests(metafunc):

    idlist = []
    argvalues = []
    for scenario in metafunc.cls.scenarios:
        idlist.append(scenario[0])
        items = scenario[1].items()
        argnames = [x[0] for x in items]
        argvalues.append([x[1] for x in items])
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")


def process_notebook(notebook_filename):
    """
    Checks if an IPython notebook runs without error from start to finish. If so, writes the
    notebook to HTML (with outputs) and overwrites the .ipynb file (without outputs).
    """

    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)

    os.path.abspath(os.path.dirname(notebook_filename))
    ep = ExecutePreprocessor(timeout=2000, kernel_name="whylogs-v1-dev", allow_errors=False)

    try:
        # Check that the notebook runs
        ep.preprocess(nb, {"metadata": {"path": os.path.join(PARENT_DIR, "python", "examples")}})
    except CellExecutionError:
        raise

    print(f"Successfully executed {notebook_filename}")
    return


class TestNotebooks:
    git_files = (
        subprocess.check_output("git ls-tree --full-tree --name-only -r HEAD", shell=True).decode("utf-8").splitlines()
    )

    # Get just the notebooks from the git files
    notebooks = [fn for fn in git_files if fn.endswith(".ipynb") and os.path.basename(fn) not in skip_notebooks]
    scenarios = [(notebook, {"notebook": notebook}) for notebook in notebooks]

    def test_all_notebooks(self, notebook):
        process_notebook(os.path.join(PARENT_DIR, notebook))
