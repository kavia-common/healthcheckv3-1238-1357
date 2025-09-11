#!/bin/bash
cd /home/kavia/workspace/code-generation/healthcheckv3-1238-1357/healthcheckv3MonolithicHealthcheckScript
source venv/bin/activate
flake8 .
LINT_EXIT_CODE=$?
if [ $LINT_EXIT_CODE -ne 0 ]; then
  exit 1
fi

