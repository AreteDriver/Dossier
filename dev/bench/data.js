window.BENCHMARK_DATA = {
  "lastUpdate": 1771419266400,
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
      },
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
          "id": "35637a33395824c6de64256dc5982530ecfb7ab9",
          "message": "ci: add CodeQL security scan workflow\n\nWeekly schedule + push/PR triggers on main. Python security-and-quality queries.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T04:45:47-08:00",
          "tree_id": "be74c015ae434fb482c387f0678d944787868ace",
          "url": "https://github.com/AreteDriver/Dossier/commit/35637a33395824c6de64256dc5982530ecfb7ab9"
        },
        "date": 1771419265522,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 216.54506037441365,
            "unit": "iter/sec",
            "range": "stddev: 0.00004458391915877021",
            "extra": "mean: 4.61797649999943 msec\nrounds: 158"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1539.5055318761488,
            "unit": "iter/sec",
            "range": "stddev: 0.000015562070275334476",
            "extra": "mean: 649.5592118992455 usec\nrounds: 1227"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.735631468652352,
            "unit": "iter/sec",
            "range": "stddev: 0.0006346505120551399",
            "extra": "mean: 72.80335107142426 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 36.562080230187355,
            "unit": "iter/sec",
            "range": "stddev: 0.00024467835866358935",
            "extra": "mean: 27.350741361109797 msec\nrounds: 36"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 215.48155791632288,
            "unit": "iter/sec",
            "range": "stddev: 0.001083510387743976",
            "extra": "mean: 4.6407683779060385 msec\nrounds: 172"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 135.9516113524073,
            "unit": "iter/sec",
            "range": "stddev: 0.0008792452718252252",
            "extra": "mean: 7.355558275862193 msec\nrounds: 87"
          }
        ]
      }
    ]
  }
}