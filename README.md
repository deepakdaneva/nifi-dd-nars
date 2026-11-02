# nifi-dd-nars

[![CI](https://github.com/deepakdaneva/nifi-dd-nars/actions/workflows/ci.yml/badge.svg)](https://github.com/deepakdaneva/nifi-dd-nars/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.deepakdaneva.nifi/nifi-dd-processors-nar?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.deepakdaneva.nifi/nifi-dd-processors-nar)
[![License](https://img.shields.io/github/license/deepakdaneva/nifi-dd-nars)](LICENSE)

Extension processors for [Apache NiFi](https://nifi.apache.org) 2.x: content-based MIME type detection, text
extraction, archive inspection and safe recursive unpacking (all backed by [Apache Tika](https://tika.apache.org) and
[Apache Commons Compress](https://commons.apache.org/proper/commons-compress/)), and removal of map cache entries.

## Contents

- [Processors](#processors)
  - [DDIdentifyMimeType](#ddidentifymimetype)
  - [DDExtractText](#ddextracttext)
  - [DDArchiveContainsFileType](#ddarchivecontainsfiletype)
  - [DDUnpackContent](#ddunpackcontent)
  - [DDRemoveDistributedMapCache](#ddremovedistributedmapcache)
  - [Error attributes](#error-attributes)
- [Project structure](#project-structure)
- [Requirements](#requirements)
- [Compatibility](#compatibility)
- [Installation](#installation)
- [Building from source](#building-from-source)
- [Contributing, security and changes](#contributing-security-and-changes)
- [License and notice](#license-and-notice)

## Processors

All processors live in the package `io.github.deepakdaneva.nifi.processors` and are shipped in
`nifi-dd-processors-nar`. Data sizes accept NiFi data size syntax (`80 MB`, `1 GB`, ...) and properties marked
*ENV* support Expression Language evaluated against environment and parameter values; *FlowFile* means FlowFile
attributes are available as well.

### DDIdentifyMimeType

Detects the MIME type of the FlowFile content with Apache Tika and sets `mime.type` and `mime.extension`.

| Property | Default | Description |
|---|---|---|
| Clean Mime Type Only | `true` | Remove parameters such as `; version=5` from the detected type. |
| Mime Config File | | Path to a custom Tika `mimetypes` XML file (*ENV*). Mutually exclusive with the config body. |
| Mime Config Body | | Content of a custom Tika `mimetypes` XML file. When a custom configuration is set, only its types are used. |

| Relationship | Description |
|---|---|
| `success` | All FlowFiles. |

| Written attribute | Description |
|---|---|
| `mime.type` | Detected MIME type (`application/octet-stream` if unknown). |
| `mime.extension` | Common extension of the type including the dot, e.g. `.pdf`; empty if unknown. |

### DDExtractText

Replaces the FlowFile content with the plain text extracted by Apache Tika (UTF-8, leading/trailing whitespace
removed). Text is streamed into the content repository, so large documents are never held in memory as a whole.

| Property | Default | Description |
|---|---|---|
| Text extraction size limit | `80 MB` | Maximum UTF-8 size of the extracted text; `-1` for no limit (*ENV*). |
| Text extraction characters limit | `100000` | Maximum number of characters; `-1` for no limit (*ENV*). |
| Fail on limit reached | `true` | `true`: route to `failure` with the original content. `false`: route to `success` with text up to the limit. |
| Tika Config File | | Path to a `tika-config.xml` (*ENV*). Mutually exclusive with the config body. |
| Tika Config Body | | Content of a `tika-config.xml`. Default Tika configuration when neither is set. |

| Relationship | Description |
|---|---|
| `success` | Content replaced by the extracted text. |
| `failure` | Text could not be extracted (encrypted, unsupported, corrupt, or limit reached with *Fail on limit reached*); content unchanged. |

| Written attribute | Description |
|---|---|
| `mime.type` | `text/plain` on success. |
| `text.extraction.limit.reached` | `true` when a limit was reached (on `success` or `failure`). |
| `deepakdaneva.exception.*` | See [Error attributes](#error-attributes). |

### DDArchiveContainsFileType

Detects the MIME type of every file inside an archive and checks whether any matches a configured list. The FlowFile
content is not modified. Supports the formats of Commons Compress (zip, jar, tar, 7z, ar, arj, cpio, dump and tar
compressed with gzip, bzip2, xz, lzma, brotli, zstd, ...).

| Property | Default | Description |
|---|---|---|
| Non Archive/Unsupported to Failure | `true` | Route non-archives to `failure` instead of `success`. |
| Recurse Sub Archives | `false` | Also search inside nested archives. |
| Fail on Sub Archives | `false` | (recursive only) Route to `failure` when a nested archive cannot be read, instead of skipping it. |
| Maximum Sub Archive Depth | `10` | (recursive only) Deepest nesting level that is opened; protects against recursive archive bombs. |
| Search Mime Types Config File | | Path to a JSON array file, e.g. `["application/pdf","text/plain"]` (*ENV*). |
| Search Mime Types Config Body | | JSON array of MIME types. Exactly one of file or body is required. |

| Relationship | Description |
|---|---|
| `success` | Searched successfully, whether or not a match was found. |
| `failure` | Could not be searched. |

| Written attribute | Description |
|---|---|
| `dd.archive.mime.type` | Detected MIME type of the FlowFile. |
| `dd.archive.subfile.match.found` | `true` / `false` (on `success`). |
| `dd.archive.subfile.match.found.type` | The matching MIME type, when found. |
| `deepakdaneva.exception.*` | See [Error attributes](#error-attributes). |

### DDUnpackContent

Unpacks an archive into one FlowFile per contained file, optionally descending into nested archives. Unpacking is
streamed and bounded:

- the *Unpack Size Threshold* counts the bytes actually decompressed, so archives that declare false or unknown entry
  sizes (zip bombs) cannot exceed it;
- nesting is limited by *Maximum Sub Archive Depth*;
- entry names are sanitized: `..`/`.` segments, leading separators and backslashes never reach the `filename` and
  `path` attributes, so downstream processors such as `PutFile` cannot be steered outside their target directory;
- a single compressed file that is not a tarball (e.g. `report.csv.gz`, `notes.txt.bz2`, `data.xz`) produces one
  FlowFile, named after the file name stored in the gzip header or, if there is none, after the parent file name
  without its compression suffix (`report.csv`).

| Property | Default | Description |
|---|---|---|
| Unpack Recursively | `false` | Unpack nested archives instead of emitting them as files. |
| Unpack Size Threshold | `80 MB` | Maximum total data unpacked from one FlowFile; `-1` for no limit (*ENV*). |
| Failure on Threshold Breach | `true` | `true`: route to `failure` and emit nothing. `false`: skip entries that do not fit. |
| Failure on Sub-Archive Unpacking | `true` | (recursive only) Route to `failure` when a nested archive cannot be unpacked, instead of skipping it. |
| Maximum Sub Archive Depth | `10` | (recursive only) Archives nested deeper are emitted as files. |
| Failure on Nothing Unpacked | `true` | Route archives that yield no files to `failure` instead of `original`. |
| Non Archive/Unsupported to Failure | `false` | Route non-archives to `failure` instead of `original`. |

| Relationship | Description |
|---|---|
| `success` | Unpacked files. |
| `original` | The original FlowFile after unpacking (or a non-archive, see above). |
| `failure` | The original FlowFile when it cannot be unpacked; no unpacked FlowFiles are emitted. |

| Written attribute | On | Description |
|---|---|---|
| `filename`, `path`, `absolute.path` | unpacked | Sanitized entry name, its directory (`dir/sub/` or `/`), and `<parent filename>/<dir>`. |
| `mime.type` | both | Detected type of the original; for unpacked files the detected type when recursive, else `application/octet-stream`. |
| `fragment.identifier`, `fragment.count` | both | Shared UUID and number of unpacked files. |
| `fragment.index` | unpacked | One-up order of the unpacked file. |
| `segment.original.filename`, `segment.path` | unpacked | Parent filename and the entry directory. |
| `file.lastModifiedTime`, `file.creationTime`, `file.owner`, `file.group`, `file.permissions` | unpacked | From tar headers only. |
| `unpack.size.threshold.breached`, `unpack.bytes` | original | Whether the threshold was hit and the bytes unpacked. |
| `deepakdaneva.exception.*` | original | See [Error attributes](#error-attributes). |

### DDRemoveDistributedMapCache

Removes a key from the map cache behind a `DistributedMapCacheClient` (e.g. `MapCacheClientService`, or the Redis
and Hazelcast map cache clients). Keys are serialized as UTF-8, matching `PutDistributedMapCache` and
`FetchDistributedMapCache`.

| Property | Default | Description |
|---|---|---|
| Cache Entry Identifier | | Key to remove (*FlowFile*). Required. |
| Distributed Cache Service | | The map cache client controller service. Required. |

| Relationship | Description |
|---|---|
| `success` | Entry removed. |
| `not-found` | No entry for the key. |
| `failure` | Blank key or cache error; the FlowFile is penalized. |

### Error attributes

When a processor routes a FlowFile because of an error it adds:

| Attribute | Description |
|---|---|
| `deepakdaneva.exception.message` | Exception message. |
| `deepakdaneva.exception.class` | Fully qualified exception class name, e.g. `io.github.deepakdaneva.nifi.exceptions.UnpackSizeThresholdReachedException`. |

## Project structure

| Module | Packaging | Role |
|---|---|---|
| `nifi-dd-processors` | jar | The processors documented above. |
| `nifi-dd-processors-nar` | nar | Bundles `nifi-dd-processors` and its libraries; parent NAR `nifi-standard-services-api-nar`. This is the NAR to install. |
| `nifi-dd-services-api` | jar | Placeholder for upcoming controller service interfaces. |
| `nifi-dd-services-api-nar` | nar | Bundles `nifi-dd-services-api`; parent NAR `nifi-standard-services-api-nar`. |
| `nifi-dd-services` | jar | Placeholder for upcoming controller service implementations. |
| `nifi-dd-services-nar` | nar | Bundles `nifi-dd-services`; parent NAR `nifi-dd-services-api-nar`. |

NAR hierarchy:

```
nifi-standard-services-api-nar          (Apache NiFi)
  +-- nifi-dd-processors-nar
  +-- nifi-dd-services-api-nar
        +-- nifi-dd-services-nar
```

The service modules contain no components yet; they define the layout for future controller services (API and
implementation, each packaged as its own NAR). See the README in each module.

## Requirements

- Apache NiFi 2.x (built and tested against NiFi **2.12.0**)
- Java 21 or newer (the Java version required by NiFi 2)

## Compatibility

| nifi-dd-nars | Apache NiFi | Java | Tika |
|---|---|---|---|
| 2.x | 2.x (built against 2.12.0) | 21+ | 3.x |
| 1.0.0 | 1.23.x (built against 1.23.2) | 11+ | 2.9 |

NiFi 2 resolves the NAR's parent (`nifi-standard-services-api-nar`) to the version installed in your NiFi when the
exact version is not present. Version 2.x keeps the processor class names, property names and relationships of
1.0.0, so existing flows load after upgrading the NAR; the new *Maximum Sub Archive Depth* properties default to `10`.

## Installation

Download the NAR and copy it into NiFi's auto-loading directory (`./extensions` by default,
`nifi.nar.library.autoload.directory`) or into `./lib` and restart NiFi.

From Maven Central:

```
https://repo1.maven.org/maven2/io/github/deepakdaneva/nifi/nifi-dd-processors-nar/<version>/nifi-dd-processors-nar-<version>.nar
```

For example:

```
VERSION=<version>
curl -fLo /opt/nifi/extensions/nifi-dd-processors-nar-$VERSION.nar \
  https://repo1.maven.org/maven2/io/github/deepakdaneva/nifi/nifi-dd-processors-nar/$VERSION/nifi-dd-processors-nar-$VERSION.nar
```

Verify the download against the `.sha1`/`.asc` files published next to the NAR.

The processor classes are also published as a plain jar (`io.github.deepakdaneva.nifi:nifi-dd-processors`) for use
in tests or custom NARs.

## Building from source

```
./mvnw -B verify
```

This compiles, runs the tests, builds `nifi-dd-processors-nar/target/nifi-dd-processors-nar-<version>.nar` and checks
formatting. Fix formatting with `./mvnw spotless:apply`. Maven 3.9+ and Java 21+ are enforced; the Maven Wrapper
downloads the right Maven version.

## Contributing, security and changes

- [CONTRIBUTING.md](CONTRIBUTING.md) - how to propose changes
- [SECURITY.md](SECURITY.md) - how to report a vulnerability
- [CHANGELOG.md](CHANGELOG.md) - release history

## License and notice

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE); the NAR carries the licenses and notices of
the third-party components it bundles in `META-INF/LICENSE` and `META-INF/NOTICE`.

Parts of `DDIdentifyMimeType` and `DDUnpackContent` are adapted from Apache NiFi's `IdentifyMimeType` and
`UnpackContent` processors (Apache License 2.0, Copyright The Apache Software Foundation); this is recorded in
[NOTICE](NOTICE) and in the NAR's `META-INF/NOTICE`.
