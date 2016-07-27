# Rusticsearch

A search server with an Elasticsearch-compatible REST API, written in [Rust](https://www.rust-lang.org)

**NOTE:** Currently, under initial research and development, please refer to [the roadmap](https://github.com/kaedroho/rusticsearch/wiki/Initial-development-roadmap) to see progress.

## Project Goals

 - Fast search with predictible resource usage
 - Rock solid reliability
 - Elasticsearch compatibility
 - Simple to install and operate

## Personal Goals

 - Build a reusable search engine library for Rust, and contribute to other Rust projects
 - Improve my Rust skills
 - Learn about search engines

## Status

Rusticsearch is in initial research/development phase. It currently supports the basic indexing/retrieval APIs
but currently lacks persistence, ranking and a few other things.

See the [roadmap](https://github.com/kaedroho/rusticsearch/wiki/Initial-development-roadmap) for a list of things
being worked on at the moment.

## Running it

It currently only compiles on Rust nightly. This can either be [downloaded from the Rust website](https://www.rust-lang.org/en-US/downloads.html) or you could use [rustup](https://github.com/rust-lang-nursery/rustup.rs) (beta).

Once rust is installed, check out the repo and run ``cargo run``:

```
git clone git@github.com:kaedroho/rusticsearch.git
cd rusticsearch
cargo run
```
