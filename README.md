# Python-LIMES

## Installation

1. Install Python3
2. Install the following packages: pandas, geopandas, shapely and SPARQLWrapper

To get faster runtime use geopandas-cython. To intall it use the commands below.
```bash
git clone https://github.com/geopandas/geopandas
git checkout geopandas-cython
make install
```

## Run

To run the program, a config file is needed. Example configs are in the config folder.

```bash
python main.py -c config.json
```