# GeoLIMES

A tool for discovery of geo-spatial links.

GeoLIMES retrieves specified properties of spatial objects from source and target datasets, through their respective SPARQL endpoints, and finds topological relations between objects in source and target objects according to topological predicates.

The specifications of the relevant properties are provided in a configuration file, which allows constraining the number of object by specifying offset and limit . A dataset can be created through properties which already exist in the graph, and, in addition, GeoLimes allows direct construction of ad-hoc values through a SPARQL select statement for a given resource.

[Source code](https://git.informatik.uni-leipzig.de/mam10kzu/Python-LIMES)

## Installation

1. Install Python3
2. Install the required libraries with `pip install -r requirements.txt`
3. Install PostgreSQL and PostGIS
4. Create a database and add extensions postgis and postgis_topology

## Measures

The following measures are supported:

- contains
- contains_properly
- covered_by
- covers
- crosses
- disjoint
- distance
- distance_within
- equals
- hausdorff_distance
- intersects
- overlaps
- touches
- within

The distance_within measure needs a threshold to work.

## Run

### Command line

To run the program, a config file and a database config file are needed. Example configs are in the configs folder.
```bash
python main.py -c config.json -d postgresql_config.json
```

### Server with Rest API

To run the server, a database config file is needed.

```bash
python server.py -d postgresql_config.json
```

Send a Post request to `serverurl:8888/limes` with a Json body, which contains the geoLIMES config.