#!/bin/bash

KACHERY_STORAGE_DIR="${KACHERY_STORAGE_DIR}"
OUTPUT_DIR=$PWD/test1_singularity_output
mkdir -p $OUTPUT_DIR
singularity exec --contain -e \
    -B $KACHERY_STORAGE_DIR:/kachery-storage \
    -B ~/.kachery:/home/.kachery \
    -B $OUTPUT_DIR:/output \
    -B /tmp:/tmp \
    docker://magland/sf-mountainsort4 \
    bash -c "KACHERY_STORAGE_DIR=/kachery-storage HOME=/home /scripts/mountainsort4.py --output /output sha1dir://3ea5c9bd992de2d27402b2e83259c679d76e9319.synth_mearec_tetrode/datasets_noise10_K10_C4/001_synth"