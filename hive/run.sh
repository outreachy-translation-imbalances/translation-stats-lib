#!/bin/bash
set -emuxo pipefail

if [ $# -eq 0 ]; then
  >&2 cat << EOS
    Parameters: one or more scripts to run
      ./run.sh 00-foo.sql 10-bar.py
EOS
  exit 1
fi

source conda-analytics-activate base
if [ -z "${DATABASE+x}" ]; then
  source .env
fi

for script in "$@"; do
  extension=${script##*.}
  if [ $extension = 'py' ]; then
    DATABASE="${DATABASE}" \
    python "$script"
  elif [ $extension = 'sql' ]; then
    spark3-sql \
      --master yarn --executor-memory 8G --executor-cores 4 --driver-memory 2G \
      --conf spark.dynamicAllocation.maxExecutors=64 \
      --database "${DATABASE}" \
      --define snapshot="${SNAPSHOT}" \
      -f "$script"
  else
    echo "Skipping ${script}"
  fi
done
