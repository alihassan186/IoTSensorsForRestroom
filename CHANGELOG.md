# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning principles where practical.

## [Unreleased]

### Added

- Rule-engine evaluation path alongside existing alert flow in `Restroomcode.py`
- Rule-engine notifications stored in MongoDB `notifications` collection with `type: "ruleengine"`
- Rule collection auto-detection support for:
  - `rules`
  - `restroomRules`
  - `rest-room rules`
- GitHub-standard project documentation and templates
