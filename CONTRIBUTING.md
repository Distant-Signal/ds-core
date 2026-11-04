# Contributing to ds-core

Start with the organisation's
[contributing guide](https://github.com/Distant-Signal/.github/blob/main/CONTRIBUTING.md):
issues, pull requests, commit identity and the conventions that apply in
every repository.

Issues are turned off here. File them in
[Distant-Signal/distant-signal](https://github.com/Distant-Signal/distant-signal/issues)
with the `area:core` label.

To work on a core change and a consumer together, patch the consumer onto
this checkout with a local Cargo `[patch]` that you don't commit
([README.md](README.md#using-it), "Using it"). The full stack's dev setup
is in distant-signal's README,
["Getting started"](https://github.com/Distant-Signal/distant-signal#getting-started).

What is specific to this repository is in [CLAUDE.md](CLAUDE.md): the
commands CI runs, the migration rules and how a release is made.
