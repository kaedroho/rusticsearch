# Rusticsearch

Lightweight Elasticsearch compatible search server.

## Why?

A good quality search engine is important for many websites and Elasticsearch provides that with an easy to use REST API. But the problem with Elasticsearch is that it requires a minimum of 2GB of memory, which makes it expensive to run.

The aim of this project is to build new search server that takes the powerful search features and simple API of Elasticsearch, but implement it in a language with more control over memory usage. We aim to keep memory usage below 100MB (excluding cache) so it should be very cheap to run.

## Project Goals

 - Decent performance with predictible resource usage
 - Focus on simplicity and stability over features
 - Elasticsearch compatibility
 - Simple to install and operate

## Why Rust?

Rust frees memory as it goes rather than leaving unused memory to be collected later by a "garbage collector" like Java. In Elasticsearch, this heap of garbage can waste gigabytes of memory that could otherwise be used as cache.

[Rust](http://www.rustlang.org/) is a systems programing language from Mozilla that's designed for building fast, secure and reliable software.

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

See [Elasticsearch query DSL support](https://github.com/kaedroho/rusticsearch/wiki/Elasticsearch-query-DSL-support).

## Running it

Rusticsearch can be compiled with Rust stable 1.15 or later. You can [download it from the Rust website](https://www.rust-lang.org/en-US/downloads.html) or you could use [rustup](https://github.com/rust-lang-nursery/rustup.rs).

Once Rust is installed, clone the repo and run ``cargo run``:

```
git clone git@github.com:kaedroho/rusticsearch.git
cd rusticsearch
cargo run
```
