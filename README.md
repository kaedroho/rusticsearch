# Rusticsearch

Lightweight Elasticsearch compatible search server.

## Why?

A good quality search engine is important for many websites and Elasticsearch provides that while also providing an easy to use RESTful API to make integrating it easier. But it does use a lot of memory which puts it out of the price range for many people.

This project aims to build a new search engine that takes the powerful search and simple API from Elasticsearch but instead implement it in a language with more control over memory, making it cheaper to run.

## Project Goals

 - Decent performance with predictible resource usage
 - Focus on simplicity and stability over features
 - Elasticsearch compatibility (where it makes sense)
 - Simple to install and operate

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

Rusticsearch can be compiled with Rust stable 1.15 or later. You can [downloaded from the Rust website](https://www.rust-lang.org/en-US/downloads.html) or you could use [rustup](https://github.com/rust-lang-nursery/rustup.rs).

Once Rust is installed, clone the repo and run ``cargo run``:

```
git clone git@github.com:kaedroho/rusticsearch.git
cd rusticsearch
cargo run
```
