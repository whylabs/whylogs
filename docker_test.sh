#! /bin/sh

git config --global --add safe.directory /workspace
export PATH=$PATH:/home/whyuser/.local/bin
WHYLABS_DEFAULT_DATASET_ID=model-2250
WHYLABS_DEFAULT_ORG_ID=org-0
WHYLABS_NO_ANALYTICS=True
WHYLABS_API_KEY=${{ secrets.WL_API_KEY }}
WHYLABS_API_ENDPOINT=${{ secrets.WL_END_POINT }}

cd /workspace/python
make install
make test pre-commit && poetry run pytest tests/api/writer/test_whylabs_integration.py --load
