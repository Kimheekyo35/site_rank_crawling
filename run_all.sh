#!/usr/bin/env bash

set -euo pipefail

python yesstyle.py
python jolse.py
python stylevana.py
python merge_all_to_email.py
