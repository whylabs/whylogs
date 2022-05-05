# Storage Format

## V1 format overview

- Delimited
- A single file can contain multiple messages

```{figure} v1_format_2.svg
:alt: Image describing delimited whylogs v1 format
:width: 500px
:align: center

Delimited format allows the new file format to pack multiple messages. A
reader can also use the chunk offsets to jump around the file to target
relevant metrics only.
```

### Overview of the message format

- DatasetHeader:
    - Properties of the dataset such as names, tags, timestamps
    - Offsets for column and dataset metrics
    - An index of metrics names
- Chunk:

Diagram for the format

```{figure} v1_format_1.svg
:alt: Image detailing the structure of v1 format
:width: 600px
:align: center

A column message might contain mulitple chunks. Using offsets, reader only needs to target
specific chunks for a column. This allows us to unify both column and dataset level metrics.
```

## Comparison with v0 format

TBD

## Migrating from v0 to v1

TBD
