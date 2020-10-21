#!bash
source /path-to-conda/etc/profile.d/conda.sh
cd path-to-project/gg-ez/

conda activate gg-ez # name of conda environment

export LC_ALL=en_US
export LANG=en_US

kedro run
