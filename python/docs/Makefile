# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build
AUTOAPI       = _autoapi/

# Put it first so that "make" without argument is like "make help".
pre-build:
	rm -f ./examples
	ln -s ../examples ./
	rm -f ./datasets
	ln -s ../whylogs/datasets/descr ./datasets
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help pre-build Makefile livehtml

build-docs:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

html: pre-build build-docs

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

livehtml: pre-build
	sphinx-autobuild --re-ignore ".*/api/.*" --ignore "$(BUILDDIR)" "$(SOURCEDIR)" "$(BUILDDIR)/html" $(SPHINXOPTS) $(O)
