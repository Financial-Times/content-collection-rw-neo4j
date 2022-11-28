# Content Collection Reader/Writer for Neo4j (content-collection-rw-neo4j)

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/Financial-Times/content-collection-rw-neo4j/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/Financial-Times/content-collection-rw-neo4j/tree/master)

An API for reading/writing content collection entities into Neo4j.
Expects the JSON supplied by the ingester.

A content collection is a component created in Methode as story package or
content package.

The service currently exposes two endpoits:

- for operations on story packages:
`http://host:port/content-collection/story-package`

- for operations on content packages:
`http://host:port/content-collection/content-package`

Functionally, the endpoints behave the same, the only difference being
the labels and relations which are saved in **Neo4j** by each.

All endpoints support the following operations:

- **GET** with an UUID will retrieve the contents and relations
of the Neo4j node with the given uuid.
The node and relation labels are dictated by the exact handler used.

e.g. the response for a GET request request to
`http://host:port/content-collection/story-package/a403a332-de48-11e6-86ac-f253db7791c6`
  
```json
{
    "uuid": "a403a332-de48-11e6-86ac-f253db7791c6",
    "items": [{
        "uuid": "d4986a58-de3b-11e6-86ac-f253db7791c6"
    },
    {
        "uuid": "d9b4c4c6-dcc6-11e6-86ac-f253db7791c6"
    },
    {
        "uuid": "d8509dc8-d7ec-11e6-944b-e7eb37a6aa8e"
    },
    {
        "uuid": "404040aa-ce97-11e6-864f-20dcb35cede2"
    },
    {
        "uuid": "834a2bc2-bd67-11e6-8b45-b8b81dd5d080"
    }],
    "publishReference": "tdi23377744",
    "lastModified": "2017-01-31T15:33:21.687Z"
}
```

In case no node with the given uuid is available, a `404` status code is returned.

- **PUT** with an UUID and a JSON payload will create a node in Neo4j
and the associated relations.
The node and relation labels are dictated by the exact handler used.
In case a node already exists, it will be updated.

e.g. a PUT request to `http://host:port/content-collection/story-package/a403a332-de48-11e6-86ac-f253db7791c6`
with the following payload:

```json
{
    "uuid": "a403a332-de48-11e6-86ac-f253db7791c6",
    "items": [{
        "uuid": "d4986a58-de3b-11e6-86ac-f253db7791c6"
    },
    {
        "uuid": "d9b4c4c6-dcc6-11e6-86ac-f253db7791c6"
    },
    {
        "uuid": "d8509dc8-d7ec-11e6-944b-e7eb37a6aa8e"
    },
    {
        "uuid": "404040aa-ce97-11e6-864f-20dcb35cede2"
    },
    {
        "uuid": "834a2bc2-bd67-11e6-8b45-b8b81dd5d080"
    }],
    "publishReference": "tdi23377744",
    "lastModified": "2017-01-31T15:33:21.687Z"
}
```

should result in a `200` status code response.

- **DELETE** with a UUID will delete the Neo4j node
with the given uuid alongside all its relations.

e.g. a DELETE request to `http://host:port/content-collection/story-package/a403a332-de48-11e6-86ac-f253db7791c6`
will result in a `204` status code if the node has been deleted or
in a `404` status code if there was no node with the given uuid.

- **GET** on the `__count` path of a handler will return
the number of nodes currently in Neo4j.
The labels of the nodes counted depend on the exact handler used.

e.g. a GET request to `http://host:port/content-collection/story-package/__count`
will return the number of story package nodes currently in Neo4j.
The response is not json formatted, it is simply a number like `10` or `0`.

## How to test

To run the full test suite of tests, you must have a running instance of Neo4j.
By default the application will look for the Neo4j instance at <bolt://Neo4j:7687>.
Otherwise you could specify a URL yourself as given by the example below:

```shell
export NEO4J_TEST_URL=bolt://Neo4j:7475
```

- Unit tests only: `go test -mod=readonly -race ./...`

In order for the integration tests to execute you must provide `GITHUB_USERNAME` and
`GITHUB_TOKEN` values, because the service is depending on internal repositories.

- Unit and integration tests:

    ```shell
    GITHUB_USERNAME="<user-name>" GITHUB_TOKEN="<personal-access-token>" \
    docker-compose -f docker-compose-tests.yml up -d --build && \
    docker logs -f test-runner && \
    docker-compose -f docker-compose-tests.yml down -v
    ```
