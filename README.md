# 👝 trove

[![tests](https://github.com/mentalblood0/trove/actions/workflows/tests.yml/badge.svg)](https://github.com/mentalblood0/trove/actions/workflows/tests.yml)

An all-indexing document database for JSON-structured data. Built on top of [dream](https://github.com/mentalblood0/dream)

## Features

- fully transactional
- can store any JSON-structured data
- immediately indexes all inserted data, maintains index automatically
- stores documents flattened, avoiding excessive overwrites on updates
- supports RAM-friendly searches by multiple path-value pairs
- implements efficient array operations `last`, `last_element_index`, `contains_element`, `push` and `get_element_index`
- can have multiple documents spaces ('buckets') for search optimization, transaction can access all the buckets
- use UUID v7 for documents yet can also use manually provided identifiers
