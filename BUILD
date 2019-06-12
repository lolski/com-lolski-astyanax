load("@graknlabs_bazel_distribution//common:rules.bzl", "java_deps", "assemble_zip")

java_binary(
    name = "storage-backend-tester",
    main_class = "com.lolski.janusgraph.StorageBackendTester",
    srcs = ["com/lolski/janusgraph/StorageBackendTester.java"],
    deps = [
        "//dependencies/artifacts/org/janusgraph:janusgraph-core",
        "//dependencies/artifacts/org/janusgraph:janusgraph-cassandra",
        "//dependencies/artifacts/org/apache/tinkerpop:gremlin-core",
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/apache/cassandra:cassandra-all"
    ],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "table-pretty-printer",
    main_class = "com.lolski.janusgraph.TablePrettyPrinter",
    srcs = ["com/lolski/janusgraph/TablePrettyPrinter.java"],
    deps = [
        "//dependencies/artifacts/com/datastax/cassandra:cassandra-driver-core",
        "//dependencies/artifacts/org/janusgraph:janusgraph-core",
        "//dependencies/artifacts/org/janusgraph:janusgraph-cassandra",
        "//dependencies/artifacts/org/apache/tinkerpop:gremlin-core",
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/apache/cassandra:cassandra-all"
    ],
)

java_deps(
    name = "deps",
    target = "storage-backend-tester",
    # target = "//dependencies/artifacts/org/apache/cassandra:cassandra-all",
    version_file = "//:VERSION"
)

assemble_zip(
    name = "zip",
    targets = [":deps"],
    additional_files = {},
    empty_directories = [],
    permissions = {},
    output_filename = "zip",
    visibility = ["//visibility:public"]
)