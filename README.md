# TreeCat: Catalog Engine for Large Data Systems

TreeCat is a single machine database engine that is designed to serve as a standalone catalog for large
data systems (data lake).

Main features of the system include:
* Hierarchical Data Model
* Flexible Schema
* Strong ACID Compliance
* Specialized Architecture (Storage Layout, Concurrency Control etc.) for Hierarchical Data

TreeCat uses RocksDB as the underlying storage engine and BSON as the data format. 
For the details of compiling TreeCat and running experiments, refer to README.md under experiments.