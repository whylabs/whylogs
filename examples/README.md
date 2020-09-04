# WhyLogs examples

This folder contains example scripts demonstrating WhyLogs usage.

## Contributing & auto documentation

This project uses [Sphinx Gallery](https://sphinx-gallery.github.io/stable/index.html) to automatically generate documentation from the scripts in this folder.  Just a few notes before adding or modifying example scripts here:

* Currently, WhyLogs cannot be installed with readthedocs.  Any script which requires WhyLogs **will not build**.
  This is due to WhyLogs' dependency on protoc which does not come with readthedocs.
* All `plot_*.py` files will be executed and documented in the gallery when producing documentation.
  Any of these files which import whylogs will fail!
* Example scripts _must_ start with a doc string like:
    ```python
    """
    Name of example
    ===============
    
    Here is a description of this example script.      
    """
    ```
* matplotlib figures and stdout will be displayed in the gallery
