import re
import os
import shutil

CONFIG = {}


def substitute(matchobj):
    """Returns string to replace for given match object (re package)."""
    if matchobj.group(1) in CONFIG.keys():
        return f'\\\"{CONFIG[matchobj.group(1)]}\\\"'
    else:
        return "None"


def generate_notebooks(project_dir, config_dict={}):
    """
    Copies notebooks from project-level notebooks directory to given project
    directory, replacing specially-marked comments with values from config.
cl
    Parameters
    ----------
    project_dir: string
        Path-like string pointing to project directory
    config_dict: dict
        Additional configuration variables
    """
    # make project notebook dir
    project_nb_path = os.path.join(project_dir, "notebooks")
    if not os.path.isdir(project_nb_path):
        os.makedirs(project_nb_path)

    # find and confirm needed files in existing package notebook dir
    package_nb_path = os.path.join(os.path.dirname(__file__), "notebooks")
    assert (os.path.isfile(os.path.join(package_nb_path, "Logging.ipynb")))
    assert (os.path.isfile(os.path.join(package_nb_path, "Analysis.ipynb")))

    # incorporate config parameter
    CONFIG["PROJECT_DIR"] = project_dir
    CONFIG.update(config_dict)

    # copy over static Logging.ipynb to project nb dir
    shutil.copyfile(os.path.join(package_nb_path, "Logging.ipynb"),
                    os.path.join(project_nb_path, "Logging.ipynb"))
    shutil.copyfile(os.path.join(package_nb_path, "lending_club_1000.csv"),
                    os.path.join(project_nb_path, "lending_club_1000.csv"))

    # replace log analysis notebook
    project_analysis_nb_path = os.path.join(project_nb_path, "Analysis.ipynb")

    if os.path.isfile(project_analysis_nb_path):
        os.remove(project_analysis_nb_path)

    with open(os.path.join(package_nb_path, "Analysis.ipynb"), 'r') as orig_nb:
        orig_analysis_lines = orig_nb.readlines()

    with open(project_analysis_nb_path, 'w') as out_nb:
        for line in orig_analysis_lines:
            out_nb.write(re.sub("###(.*)###", substitute, line))
