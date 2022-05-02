# v0 to v1 Basics

## Performance Improvement

* Native integration with

### Columnar Operations

- We prioritized columnar operations

### Microbatching by Default

- Single operations are batched into micro batches

## Extensible APIs

- Flexible naming
- Extension hooks for custom metrics

## Profile vs View

- New View concept: what it is
- Why?

## New format

Check out our [guide on storage format](format) for more information around the new storage format.

## Logger API

- Simplified logger API
- Simpler way to write whylogs

````{tab} v0
```python
import whylogs
session = whylogs.get_or_create_session()
```

It involves multiple concepts here. In v1, we simplify the number of entities you need to configure at first.

Check out the {py:class}`whylogs..ResultSet`
````

````{tab} v1
```python
import whylogs
session = whylogs.get_or_create_session()
```

It's pretty simple! Check out the {py:class}`whylogs.ResultSet`
````
