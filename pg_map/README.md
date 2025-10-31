# Map

A PostgreSQL extension to emulate a key/value "map", supporting all data types that exist in PostgreSQL, but in-built and user-created.

## Core Concept

Rather than inventing a new type from scratch, we piggyback on the existing functionality of PostgreSQL [composite types](https://www.postgresql.org/docs/current/rowtypes.html).

A map is structured as an array of key value pairs. The N'th key corresponds to the N'th value.

```sql
CREATE TYPE map_type.pair_text_int AS (key text[], val integer[]);
CREATE DOMAIN map_type.key_text_val_int AS map_type.pair_text_int[]; 
```

To provide a little structure around the maps, each map type will be declared with a type name using the structure `key_<keytype>_val_<valuetype>`.

```sql
SELECT array[('me', 1), ('myself', 2), ('i', 3)]::map_type.key_text_val_int;
```

Because there is a huge number of potential type combinations that could make up a map, new map types will have to be declared at the time of use.

## Creating a New Map

Declaring a new map type done with a utility function that both creates the type, and also all the support functions needed to make use of that type.

```sql
SELECT map_type.create('text', 'integer');
```

The create function not only creates the type, it also creates all the functions that operate on the type. This is so the PostgreSQL typing system can make sense of the inputs and outputs of the functions.

For example, for the `map_type.key_text_val_integer` type, a function `map_type.extract(map key_text_val_integer, key text)` returning `integer` will also be created.

## Functions

* `map_type.create(keytype text, valuetype text)` returns `boolean` creates a new map type and all other supporting functions for that type.
* `map_type.extract(maptype, keytype)` returns `valuetype` allows you to pull a value out of a map using a key.
* `map_type.cardinality(maptype)` returns `integer` returns the size of the map (or the number of entries in the map).
* `map_type.entries(maptype)` returns a set of key and value pairs

## For Reference

### DuckDB API

cardinality(map) returns integer
  Return the size of the map (or the number of entries in the map).

element_at(map, key)

map_entries(map)
  Return a list of struct(k, v) for each key-value pair in the map.

map_extract(map, key)
  Alias of element_at

map_from_entries(STRUCT(k, v)[])

map_keys(map)
  Return a list of all keys in the map

map_values(map)
  Return a list of all values in the map.

map()
  Returns an empty map

map[entry]
  Alias for element_at



