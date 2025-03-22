# HelixDB

HelixDB is a multi-model database built for performance and simplicity.

## Overview

HelixDB is a high-performance database system designed with a focus on developer experience and efficient data operations. Built in Rust and powered by LMDB as its storage engine, it combines the reliability of a proven storage layer with modern features tailored for AI and vector-based applications.

## Key Features

- **Fast & Efficient**: Built for performance with lightning-fast startup times and millisecond query latency
- **Vector-First**: Native support for vector data types, making it ideal for RAG (Retrieval Augmented Generation) and AI applications
- **Developer Friendly**: Intuitive query language with built-in type checking and easy-to-use build tools
- **Reliable Storage**: Powered by LMDB (Lightning Memory-Mapped Database) for robust and efficient data persistence
- **ACID Compliant**: Ensures data integrity and consistency
- **Managed Service**: Available as a fully managed cloud service for simplified operations

## Getting Started

#### Helix CLI

The Helix CLI tool can be used to check, compile and deploy Helix locally.

1. Install CLI

   ```bash
   curl -sSL "https://install.helix-db.com" | bash
   ```

2. Install Helix

   ```bash
   helix install
   ```

3. Setup

   ```bash
   helix init --path <path-to-create-files-at>
   ```

4. Write queries

   Write your schema and queries in the newly created `.hx` files.
   Head over to [our GitHub](https://github.com/HelixDB/helix-db) for more information about writing queries

5. Check your queries (optional)

   ```bash
   cd <path-to-your-project>
   helix check
   ```

6. Deploy your queries

   ```bash
   cd <path-to-your-project>
   helix deploy --local
   ```

Other commands:

- `helix instances` to see your local instances.
- `helix stop <instance-id>` to stop your local instances.
- `helix stop --all` to stop all your local instances.
- `helix start <instance-id>` to start your local instances.

## Roadmap

Our current focus areas include:

- Expanding vector data type capabilities for AI/ML applications
- Enhancing the query language with robust type checking
- Improving build tools and developer experience
- Implementing an easy-to-use testing system via CLI
- Optimizing performance for core operations

## License

HelixDB is licensed under the GNU General Public License v3.0 (GPL-3.0).

## Commercial Support

HelixDB is available as a managed service. Contact us for more information about enterprise support and deployment options.
