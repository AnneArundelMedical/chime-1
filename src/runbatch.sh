#!/bin/sh

COPY_DEST='\\aamcvepcndw01\D$\'

py batch.py "$@" \
cp "$(cat OUTPUT_FILENAME.txt)" "$COPY_DEST"

