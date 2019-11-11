#!/bin/bash

PREFIX=$1
RESULT_DIR=$2


if [ -z "$RESULT_DIR" ]
then
    echo "[ERROR] RESULT_DIR is empty."
    echo "[ERRPR] You need to assign a dir name to process."
else

    if [ -z "$PREFIX" ]; then
        echo "[ERROR] $PREFIX is empty."
        echo "[ERRPR] You need to assign a prefix to process."
    else
        if [ ! -d "target" ]
        then
            mkdir target
        fi

        for f in $(hadoop fs -ls $RESULT_DIR/*.csv | sed 's/\s\+/ /g' | cut -d ' ' -f 8); do
            hadoop fs -cat $f >> target/$PREFIX-result.folded
        done

        ./sbin/flamegraph.pl target/$PREFIX-result.folded > target/$PREFIX-fg.svg
    fi
fi
