#!/bin/bash
cd "$(dirname "$0")"
. .venv/bin/activate
python scripts/backfill_emails.py --max-emails 100
