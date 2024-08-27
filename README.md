# Spatial Concurrent Hash Trie Map

## This Project
`SpatialConcurrentTrieMap` is an adaptation of [romix / java-concurrent-hash-trie-map](https://github.com/romix/java-concurrent-hash-trie-map) that uses H3 cell identifiers as the map hierarchical keys. It allows performing various spatial operation at any resolution. This data structure keeps all properties of the original implementation.<br />
`Tracker` is a demonstration of how a set of moving objects may be tracked and searched using the basic map as an underlying storage.

Please note, that since H3 cell id has more levels than an average hash trie (base cell + 15 resolution levels), basic map operations are expected to be slower that in the original implementation. Performance test results to compare various implementations will be added later.

A detailed tech writeup about this implementation may be found at [medium.com](https://medium.com/@lonelylockley/combining-h3-hexagons-and-ctries-for-effective-spatial-search-eafedb9a8dc8)

## Main Terms Used In The Implementation
- CellId: An H3 cell id with an arbitrary resolution in a form of a *long* number, ar hexadecimal *string*. It is believed that the best performance results may be achieved by using r15 for objects stored in the map. In this case update contention should be lower and you'll be able to choose any suitable resolution in search requests.
- BusinessEntityID: A unique identifier of a business entity that will be used as a map value. It is used to distinct different values in case of a cell collision.

## What is H3

H3 stands for "Hexagonal Hierarchical Spatial Index" - a geospatial indexing system developed by Uber. This system is designed to efficiently manage and analyze geospatial data by dividing the world into hexagonal cells, each with a unique identifier. These hexagons have several advantages over traditional grid systems:
- **Hexagonal Shape**: Unlike square grids, hexagons have a consistent distance between their centers, making spatial analysis more accurate and uniform.
- **Hierarchical Indexing**: H3 supports multiple resolutions, allowing data to be indexed at different levels of detail. Each hexagon can be subdivided into finer hexagons, facilitating both coarse and fine-grained spatial analyses.
- **Efficient Geospatial Queries**: The system enables efficient spatial queries, such as finding nearby points of interest, aggregating data over regions, and performing spatial joins.

[More info about H3 System](https://www.uber.com/en-TR/blog/h3/) <br />
[H3 CellId Bit Layout](https://h3geo.org/docs/core-library/h3indexing)

## Disclaimer

This code is writted as a demonstration of an idea how H3 hierarchy in a form of a prefix-tree may be utilized for the efficient spatial search. This implementation is not distributed as a maven dependency currently.

## License

Copyright [2024] [Alexey Zaytsev <lonelylockley@gmail.com>]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
