"""Privacy and access-control building blocks (ticket #25).

* ``registry``      loads and validates ``governance/data_classification.yaml``
* ``tokenise``      deterministic keyed HMAC pseudonymisation of identifiers
* ``generalise``    birth-year / age / date generalisation
* ``scrub``         free-text scrubbing of obvious identifiers
* ``views``         analyst-facing masked views (DuckDB dialect)
* ``snowflake_sql`` generates the Snowflake tag-assignment scripts from the registry

All data in this repo is synthetic. Nothing here is a HIPAA compliance claim.
"""
