# Rusticsearch

A search server with an Elasticsearch-compatible REST API, written in [Rust](https://www.rust-lang.org)

**NOTE:** Currently, under initial research and development, please refer to [the roadmap](https://github.com/kaedroho/rusticsearch/wiki/Initial-development-roadmap) to see progress.

## Project Goals

 - Decent performance with predictible resource usage
 - Focus on simplicity and stability over features
 - Secure out of the box
 - Elasticsearch compatibility (where it makes sense)
 - Simple to install and operate

## Personal Goals

 - Build a reusable search engine library for Rust, and contribute to other Rust projects
 - Improve my Rust skills
 - Learn about search engines

## Status

Rusticsearch is in its initial research/development phase. It currently supports basic indexing and retrieval
but lacks persistence and many other things.

See the [roadmap](https://github.com/kaedroho/rusticsearch/wiki/Initial-development-roadmap) for a list of things
being worked on at the moment.

## Running it

Rusticsearch can be compiled with the latest stable version of Rust (it may work with older versions as well). This can either be [downloaded from the Rust website](https://www.rust-lang.org/en-US/downloads.html) or you could use [rustup](https://github.com/rust-lang-nursery/rustup.rs) (beta).

Once Rust is installed, clone the repo and run ``cargo run``:

```
git clone git@github.com:kaedroho/rusticsearch.git
cd rusticsearch
cargo run
```
