import os
import shutil
import subprocess
import venv

import nbformat
import papermill as pm
import pytest

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.join(TEST_DIR, os.pardir, os.pardir)
OUTPUT_NOTEBOOK = "output.ipynb"
skip_notebooks = [
    "Guest Session.ipynb",
    "Single_Image_Tracing_Profile_to_WhyLabs.ipynb",
    "Pyspark_Profiling.ipynb",
    "WhyLabs_Sagemaker-PyTorch.ipynb",
    "Kafka_Example.ipynb",
    "Writing_to_WhyLabs.ipynb",
    "Writing_Reference_Profiles_to_WhyLabs.ipynb",
    "flask_with_whylogs.ipynb",
    "BigQuery_Example.ipynb",
    "Segments.ipynb",
    "Writing_Regression_Performance_Metrics_to_WhyLabs.ipynb",
    "Writing_Classification_Performance_Metrics_to_WhyLabs.ipynb",
    "Getting_Started_with_WhyLabsV1.ipynb",
    "Getting_Started_with_UDFs.ipynb",
    "Writing_Feature_Weights_to_WhyLabs.ipynb",
    "Image_Logging.ipynb",
    "Writing_Ranking_Performance_Metrics_to_WhyLabs.ipynb",
    "Image_Logging_Udf_Metric.ipynb",
    "mnist_exploration.ipynb",
    "performance_estimation.ipynb",
    "Embeddings_Distance_Logging.ipynb",  # skipped due to data download
    "whylogs_Audio_examples.ipynb",  # skipped because of Kaggle data download and API key for whylabs upload
    "Logging_with_Debug_Events.ipynb",  # skipped because of API key required with whylabs writing
    "NLP_Summarization.ipynb",
    "Multi dataset logger.ipynb",
    "Pyspark_and_Constraints.ipynb",
    "Feature_Stores_and_whylogs.ipynb",
    "LocalStore_with_Constraints.ipynb",  # skipped because it has over 4 minutes of thread.sleep in it
    "KS_Profiling.ipynb",  # skipped because this takes a few minutes to run
    "Monitoring_Embeddings.ipynb",  # skipped because needs user input
    "whylogs_UDF_examples.ipynb",  # skipped until multiple output column UDFs released
    "Transaction_Examples.ipynb",  # skipped because API key required for whylabs writing
    "Basics_of_Custom_Performance_Metrics.ipynb",  # skipped because API key required for whylabs writing
]

# Collect notebooks to test
git_files = (
    subprocess.check_output("git ls-tree --full-tree --name-only -r HEAD", shell=True).decode("utf-8").splitlines()
)
notebooks = [fn for fn in git_files if fn.endswith(".ipynb") and os.path.basename(fn) not in skip_notebooks]
venv_dirs = [os.path.join(TEST_DIR, f"venv_{os.path.basename(nb)}") for nb in notebooks]


def pre_install_packages(notebook_path, venv_dir):
    # Read the notebook and extract the pip commands
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
    pip_commands = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            lines = cell["source"].splitlines()
            for line in lines:
                if line.startswith("%pip install") or line.startswith("!pip install"):
                    pip_commands.append(line[1:])

    if pip_commands:
        activate_script = os.path.join(venv_dir, "bin", "activate")
        if not os.path.exists(activate_script):
            raise FileNotFoundError(f"Activation script not found: {activate_script}")
        # Combine pip commands with `&&` after activating the venv
        combined_commands = " && ".join(pip_commands)
        command = f"source {activate_script} && {combined_commands}"
        try:
            print(f"Running command: {command}")
            result = subprocess.run(command, shell=True, executable="/bin/bash", capture_output=True, text=True)
            print(result.stdout)
            print(result.stderr)
            result.check_returncode()
        except subprocess.CalledProcessError as e:
            print(f"Failed to install packages for {notebook_path} with exception: {e}")
            raise


def run_notebook_test(notebook, venv_dir):
    notebook_path = os.path.join(PARENT_DIR, notebook)
    # Activate the virtual environment
    activate_script = os.path.join(venv_dir, "bin", "activate")
    if not os.path.exists(activate_script):
        raise FileNotFoundError(f"Activation script not found: {activate_script}")
    command = f"source {activate_script} && python -m pip install --upgrade pip"
    subprocess.check_call(command, shell=True, executable="/bin/bash")
    # Execute the notebook
    try:
        pm.execute_notebook(
            notebook_path,
            OUTPUT_NOTEBOOK,
            kernel_name="python3",
            parameters=dict(venv_path=venv_dir),
        )
    except Exception as e:
        print(f"Notebook: {notebook} failed test with exception: {e}")
        raise
    print(f"Successfully executed {notebook}")


@pytest.mark.parametrize("notebook, venv_dir", zip(notebooks, venv_dirs))
def test_all_notebooks(notebook, venv_dir):
    # Setup: Create a virtual environment for the notebook since these install various packages
    os.makedirs(venv_dir, exist_ok=True)
    if not os.listdir(venv_dir):
        venv.create(venv_dir, with_pip=True)
    else:
        print(f"Using existing virtual environment in {venv_dir}")
    # Verify the virtual environment is created correctly
    bin_dir = os.path.join(venv_dir, "bin")
    if not os.path.exists(bin_dir):
        raise RuntimeError(f"Failed to create virtual environment in {venv_dir}")
    else:
        print(f"Virtual environment checked successfully in {venv_dir}")
    notebook_path = os.path.join(PARENT_DIR, notebook)
    # Before the notebook runs, extract and run the install commands so we have updated packages
    pre_install_packages(notebook_path, venv_dir)

    try:
        run_notebook_test(notebook, venv_dir)
    finally:
        shutil.rmtree(venv_dir, ignore_errors=True)
