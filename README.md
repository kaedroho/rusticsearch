# Rusticsearch

A search server with an Elasticsearch-compatible REST API, written in [Rust](https://www.rust-lang.org)

## Project Goals

 - Decent performance with predictible resource usage
 - Focus on simplicity and stability over features
 - Elasticsearch compatibility (where it makes sense)
 - Simple to install and operate

## Personal Goals

 - Build a reusable search engine library for Rust, and contribute to other Rust projects
 - Improve my Rust skills
 - Learn about search engines

## Status

Please consider this project pre-alpha quality. It currently only supports a subset of Elasticsearch's APIs
which is probably not enough to run most applications.

It currently supports indexing, both in bulk, and individually (However, the bulk indexer is quite slow at the moment),
and searching using the BM25 similarity algorithm.

See the [roadmap](https://github.com/kaedroho/rusticsearch/wiki/Initial-development-roadmap) for a list of things
being worked on at the moment.

### TODO before first alpha release

 - [ ] Make bulk indexing API faster (It currently indexes each document individually, instead of batching)
 - [ ] Implement persistence for analyzers and aliases
 - [ ] Implement a method of configuring the server from an external configuration file

### Elasticsearch compatibility

See [Elasticsearch query DSL support] (https://github.com/kaedroho/rusticsearch/wiki/Elasticsearch-query-DSL-support).

## Running it

Rusticsearch can be compiled with the latest stable version of Rust (it may work with older versions as well). This can either be [downloaded from the Rust website](https://www.rust-lang.org/en-US/downloads.html) or you could use [rustup](https://github.com/rust-lang-nursery/rustup.rs).

Once Rust is installed, clone the repo and run ``cargo run``:

```
git clone git@github.com:kaedroho/rusticsearch.git
cd rusticsearch
cargo run
```
