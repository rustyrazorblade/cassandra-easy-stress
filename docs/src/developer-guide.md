# Developer Guide

## Building the Documentation

The documentation uses [mdbook](https://rust-lang.github.io/mdBook/). To build locally:

1. Install mdbook:
   ```bash
   cargo install mdbook mdbook-admonish
   ```

2. Generate the command examples:
   ```bash
   $ manual/generate_examples.sh
   ```

3. Build and serve the documentation:
   ```bash
   $ mdbook serve docs
   ```

The documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch.

## Writing a Custom Workload

`cassandra-easy-stress` is a work in progress. Writing a stress workload isn't documented yet as it is still changing.
