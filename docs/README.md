<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB Marisa Extension by [Query.Farm](https://query.farm)

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/marisa.html)
[![v1.5 build](https://github.com/Query-farm/marisa/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/marisa/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **Marisa** extension, developed by **[Query.Farm](https://query.farm)**, adds [MARISA](https://github.com/s-yata/marisa-trie) (Matching Algorithm with Recursively Implemented StorAge) trie functionality for DuckDB. [MARISA](https://github.com/s-yata/marisa-trie) is a static and space-efficient trie data structure that enables fast string lookups, prefix searches, and predictive text operations.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/marisa](https://query.farm/products/extensions/marisa)**

## Installation

```sql
INSTALL marisa FROM community;
LOAD marisa;
```
