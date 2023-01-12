import os
import subprocess

import papermill as pm

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.join(TEST_DIR, os.pardir, os.pardir)
OUTPUT_NOTEBOOK = "output.ipynb"
skip_notebooks = [
    "Pyspark_Profiling.ipynb",
    "Kafka_Example.ipynb",
    "Writing_to_WhyLabs.ipynb",
    "Writing_Reference_Profiles_to_WhyLabs.ipynb",
    "flask_with_whylogs.ipynb",
    "BigQuery_Example.ipynb",
    "Segments.ipynb",
    "Writing_Regression_Performance_Metrics_to_WhyLabs.ipynb",
    "Writing_Classification_Performance_Metrics_to_WhyLabs.ipynb",
    "Getting_Started_with_WhyLabsV1.ipynb",
    "Writing_Feature_Weights_to_WhyLabs.ipynb",
    "Image_Logging.ipynb",
    "mnist_exploration.ipynb",
]


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
    try:
        pm.execute_notebook(notebook_filename, OUTPUT_NOTEBOOK, timeout=180)
    except Exception as e:
        print(f"Notebook: {notebook_filename} failed test with exception: {e}")
        raise

    print(f"Successfully executed {notebook_filename}")


class TestNotebooks:
    git_files = (
        subprocess.check_output("git ls-tree --full-tree --name-only -r HEAD", shell=True).decode("utf-8").splitlines()
    )

    # Get just the notebooks from the git files
    notebooks = [fn for fn in git_files if fn.endswith(".ipynb") and os.path.basename(fn) not in skip_notebooks]
    scenarios = [(notebook, {"notebook": notebook}) for notebook in notebooks]

    def test_all_notebooks(self, notebook):
        process_notebook(os.path.join(PARENT_DIR, notebook))
