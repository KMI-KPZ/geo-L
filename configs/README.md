# Configs

Two configs are needed to run geo-L, a database config and a run config.

## Database config

Config for connecting to a PostgreSQL database.

```json
{
    "database_name": string,        // required, name of the database
    "database_user": string,        // required, name of the database user
    "database_password": string,    // required, password for the database user
    "database_host": string,        // required, host address of the database server
    "database_port": integer        // required, port of the database server
}
```

## Run config

There are two different types of run configs.

With raw query:

```json
{
    "source": {
        "id": string,           // required, id for source
        "rawquery": string,     // required, sparql query
        "endpoint": string,     // required, uri for endpoint
        "var": {
            "shape": string,    // required, variable for shape
            "uri": string       // required, variable for uri
        },
        "offset": integer,      // optional, offset to result
        "limit": integer,       // optional, limit for result
        "chunksize": integer    // required, size for for results to download at once (server can limit to smaller chunksize)
    },
    "target": {
        "id": string,           // required, id for target
        "rawquery": string,     // required, sparql query
        "endpoint": string,     // required, uri for endpoint
        "var": {
            "shape": string,    // required, variable for shape
            "uri": string       // required, variable for uri
        },
        "offset": integer,      // optional, offset to result
        "limit": integer,       // optional, limit for result
        "chunksize": integer    // required, size for for results to download at once (server can limit to smaller chunksize)
    },
    "measure": {
        "relation": string,     // required, measure method
        "threshold": float      // optional, some measures require a threshold
    },
    "output_format": string     // required, specifies the output format
}
```

Without raw query:

```json
{
    "prefixes": [
        {
            "label": string,    // required, label for namespace
            "namespace": string // required, uri for namespace
        }
    ],
    "source": {
        "id": string,           // required, id for source
        "graph": string,        // required, uri for graph
        "endpoint": string,     // required, uri for endpoint
        "var": {
            "shape": string,    // required, variable for shape
            "uri": string       // required, variable for uri
        },
        "property": string  ,   // required, where property
        "offset": integer,      // optional, offset to result
        "limit": integer,       // optional, limit for result
        "chunksize": integer    // required, size for for results to download at once (server can limit to smaller chunksize)
    },
    "target": {
        "id": string,           // required, id for target
        "graph": string,        // required, uri for graph
        "endpoint": string,     // required, uri for endpoint
        "var": {
            "shape": string,    // required, variable for shape
            "uri": string       // required, variable for uri
        },
        "property": string,     // required, where property
        "offset": integer,      // optional, offset to result
        "limit": integer,       // optional, limit for result
        "chunksize": integer    // required, size for for results to download at once (server can limit to smaller chunksize)
    },
    "measure": {
        "relation": string,     // required, measure method
        "threshold": float      // optional, some measures require a threshold
    },
    "output_format": string     // required, specifies the output format
}
```
