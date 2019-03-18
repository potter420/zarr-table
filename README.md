# zarr-table
Tabular Dataformat on Zarr, Optimized for Windows

## Architecture Direction
# Table
- Each Table is stored as collections of columns (Columnar Storage)
- Each Column is stored as Zarr Array, can be multi dimensional
