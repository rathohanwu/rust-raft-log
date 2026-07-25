# Repository conventions

## Rust test layout

- Keep production modules and their unit tests in separate files.
- For `src/<area>/<module>.rs`, place that module's unit tests in
  `src/<area>/<module>/tests.rs` and declare them from the module with:

  ```rust
  #[cfg(test)]
  mod tests;
  ```

- Example: `src/storage/log.rs` must use
  `src/storage/log/tests.rs` for its unit tests.
- Do not add inline `#[cfg(test)] mod tests { ... }` blocks to production
  module files.
- When touching an existing module with inline unit tests, move those tests to
  its matching `tests.rs` file as part of the change.
- Keep cross-module, process, network, and end-to-end tests under the root
  `tests/` directory. This convention applies to module-local unit tests only.
