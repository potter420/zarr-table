# zarr-table
Tabular Dataformat on Zarr, Optimized for Windows

# Design Documents
## Table
- Each Table is stored as collections of columns (Columnar Storage)
- Each Column is stored as Zarr Array, can be multi dimensional
- Support indexing by column name, list, numpy index, array index
- Support basic table operation: Join, Group By, Aggregates
- More idea here...

## Schema
- Schema is a collection of tables
