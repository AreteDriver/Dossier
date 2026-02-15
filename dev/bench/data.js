window.BENCHMARK_DATA = {
  "lastUpdate": 1771151484205,
  "repoUrl": "https://github.com/AreteDriver/Dossier",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "AreteDriver@users.noreply.github.com",
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "committer": {
            "email": "AreteDriver@users.noreply.github.com",
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "distinct": true,
          "id": "95035abd7c0c28d0829b28334fa13172be1d0108",
          "message": "perf: add pytest-benchmark suite with CI regression gates\n\n6 benchmarks: NER extraction (5K words), FTS5 search (500 docs),\nentity resolution (200 entities), betweenness centrality (100 nodes),\ncommunity detection (100 nodes), bulk insert (100 docs).\n\nCI: benchmark-action/github-action-benchmark with 15% regression\nthreshold, auto-push to gh-pages, fail-on-alert for PRs.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-15T02:26:35-08:00",
          "tree_id": "ba131c32d085349fd92a4d18729c51b6e1f9710e",
          "url": "https://github.com/AreteDriver/Dossier/commit/95035abd7c0c28d0829b28334fa13172be1d0108"
        },
        "date": 1771151483486,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 216.5420742628921,
            "unit": "iter/sec",
            "range": "stddev: 0.000042505558790169085",
            "extra": "mean: 4.618040181816831 msec\nrounds: 154"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1535.8329032071351,
            "unit": "iter/sec",
            "range": "stddev: 0.000012540364659663484",
            "extra": "mean: 651.1124992255305 usec\nrounds: 1292"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.837336614671484,
            "unit": "iter/sec",
            "range": "stddev: 0.00017932789361519807",
            "extra": "mean: 72.26824264285929 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 36.44767564698392,
            "unit": "iter/sec",
            "range": "stddev: 0.00012975581596482813",
            "extra": "mean: 27.436591833332752 msec\nrounds: 36"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 213.95819549900366,
            "unit": "iter/sec",
            "range": "stddev: 0.0009426088071051919",
            "extra": "mean: 4.673810216373117 msec\nrounds: 171"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 128.11085254872773,
            "unit": "iter/sec",
            "range": "stddev: 0.0012873947347761207",
            "extra": "mean: 7.805739951809657 msec\nrounds: 83"
          }
        ]
      }
    ]
  }
}