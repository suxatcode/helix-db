<p align="center">
  <img src="./icon-1.png" alt="HelixDB Logo" width="200" height="200">
</p>

# HelixDB

HelixDB is a Rust written, open-source, graph-vector database built for RAG and AI applications.


## Overview

HelixDB is a high-performance database system designed with a focus on developer experience and efficient data operations. Built in Rust and powered by LMDB as its storage engine, it combines the reliability of a proven storage layer with modern features tailored for AI and vector-based applications.

We are currently using LMDB via Heed, a rust wrapper built by the amazing team over at [Meilisearch](https://github.com/meilisearch/heed).

## Key Features

- **Fast & Efficient**: Built for performance we're currently 1000x faster than Neo4j, 100x faster than TigerGraph and on par with Qdrant for vectors.
- **RAG-First**: Native support for graph and vector data types, making it ideal for RAG (Retrieval Augmented Generation) and AI applications
- **Graph-Vector**: Easiest database for storing relationships between nodes, vectors, or nodes AND vectors.
- **Reliable Storage**: Powered by LMDB (Lightning Memory-Mapped Database) for robust and efficient data persistence
- **ACID Compliant**: Ensures data integrity and consistency

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

   Open your newly created `.hx` files and start writing your schema and queries.
   Head over to [our docs]([https://github.com/HelixDB/helix-db](https://docs.helix-db.com/introduction/cookbook/basic)) for more information about writing queries
```js
QUERY addUser(name: String, age: Integer) =>
    user <- AddN<User({name: name, age: age})
    RETURN user

QUERY getUser(user_name: String) =>
    user <- N<User::WHERE(_::{name}::EQ(user_name))
    RETURN user
```
   
6. Check your queries compile before building them into API endpoints (optional)

   ```bash
   cd <path-to-your-project>
   helix check
   ```

7. Deploy your queries

   ```bash
   cd <path-to-your-project>
   helix deploy --local
   ```
8. Start calling them using our [TypeScript SDK](https://github.com/HelixDB/helix-ts) or [Python SDK](https://github.com/HelixDB/helix-py)

Other commands:

- `helix instances` to see all your local instances.
- `helix stop <instance-id>` to stop your local instance with specified id.
- `helix stop --all` to stop all your local instances.
- `helix start <instance-id>` to start your local instance with specific id.

## Roadmap

Our current focus areas include:

- Expanding vector data type capabilities for AI/ML applications
- Enhancing the query language with robust type checking
- Improving build tools and developer experience
- Implementing an easy-to-use testing system via CLI
- Optimizing performance for core operations

## License

HelixDB is licensed under the The AGPL (Affero General Public License).

## Commercial Support

HelixDB is available as a managed service for selected users, if you're interested in using Helix's managed service or want enterprise support contact us for more information and deployment options.
