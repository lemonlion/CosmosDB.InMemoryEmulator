# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.0.5] - 2026-04-23

### Fixed
- Document-level 404 (NotFound) exception messages now include "Resource Not Found" to match real Cosmos DB SDK behavior, enabling code that checks `e.Message.Contains("Resource Not Found")` to work correctly with the emulator
- Stream API 404 responses for document CRUD operations now include a JSON error body (`{"code":"NotFound","message":"Resource Not Found. ..."}`) matching the real Cosmos DB REST API format, so `CosmosException.Message` through FakeCosmosHandler contains the expected error text

## [2.0.189] - 2026-04-18

### Added
- NuGet package icon for all three packages

## [2.0.188] and earlier

See [GitHub Releases](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/releases) for previous changes.
