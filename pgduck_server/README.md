# pgduck_server

`pgduck_server` is a specialized server application designed to interface with clients using the PostgreSQL protocol. It by default listens on port 5332 on localhost and transparently executes queries on DuckDB. This server is designed for scenarios where you want to leverage DuckDB's capabilities while maintaining compatibility with PostgreSQL client applications.

## Features

- **PostgreSQL Protocol Compatibility:** Clients can communicate with `pgduck_server` using the standard PostgreSQL protocol.
- **DuckDB Integration:** Queries received by `pgduck_server` are executed on DuckDB, an in-process SQL OLAP database management system.
- **Default Listening on Port 5332:** The server listens on port 5332 on localhost by default, making it easy to set up and test.

## Getting Started

- Compile the project
- Execute the `pgduck_server` binary
- Connect to the server via `psql` or any Postgres client 

### Prerequisites

What things you need to install the software and how to install them:

- A compatible operating system (Linux, macOS, Windows)
- Necessary libraries and dependencies for DuckDB and PostgreSQL


### Code convention

- **Indentation:** We always indent the code before commits via https://github.com/postgres/postgres/tree/master/src/tools/pgindent. Please follow pgindent documentation for the details.

 - **Naming Conventions:**
    - *Variables and Functions:* Use lower case with underscores (e.g., `current_connection`, `execute_query`).
    - *Macros and Constants:* Upper case with underscores (e.g., `MAX_CONNECTIONS`, `DEFAULT_TIMEOUT`).
    - *Structs and Enums:* Use CamelCase (e.g., `BufferTag`).
    - *Global Variables:* Include a module identifier as a prefix (e.g., `BufferDescriptors`).

  - **Commenting Style:**
    - *Function Comments:* Describe the purpose, arguments, return value, and side effects at the beginning of each function.
    - *In-line Comments:* Focus on explaining "why" rather than "what"; keep them concise.
    - *Block Comments:* Reserved for explaining complex algorithms or decisions.
    - *File Header Comments:* Each file should start with a comment describing its contents and purpose.

## Running the tests

- Simply run `make check`
- Make sure `pytest` is installed
