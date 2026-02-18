window.BENCHMARK_DATA = {
  "lastUpdate": 1771445938412,
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
          "id": "a47dbee422bcc53def76a5ae4d9d2f1e6ed2cd17",
          "message": "fix: resolve 12 CodeQL alerts (empty-except, string-concat, repeated-import)\n\n- Narrow email date parse catch to (ValueError, TypeError) with comment\n- Wrap 5 implicit string concatenations in test_benchmarks.py with parens\n- Remove 6 redundant inner imports in test_cli.py (already at module level)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T12:12:21-08:00",
          "tree_id": "540f5a00e5503dfa8a79c56997995a0a503d6c76",
          "url": "https://github.com/AreteDriver/Dossier/commit/a47dbee422bcc53def76a5ae4d9d2f1e6ed2cd17"
        },
        "date": 1771445646351,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 217.13753064656683,
            "unit": "iter/sec",
            "range": "stddev: 0.00004829605247841487",
            "extra": "mean: 4.605376127389478 msec\nrounds: 157"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1556.5914468073615,
            "unit": "iter/sec",
            "range": "stddev: 0.000015432556425648955",
            "extra": "mean: 642.4293298354199 usec\nrounds: 1334"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.682098574039555,
            "unit": "iter/sec",
            "range": "stddev: 0.00015307000644468736",
            "extra": "mean: 73.08820314285721 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 35.806564235931084,
            "unit": "iter/sec",
            "range": "stddev: 0.00018929041738697386",
            "extra": "mean: 27.92784008571597 msec\nrounds: 35"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 213.53807738393684,
            "unit": "iter/sec",
            "range": "stddev: 0.0011040347453492674",
            "extra": "mean: 4.683005542856986 msec\nrounds: 175"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 112.75365645544717,
            "unit": "iter/sec",
            "range": "stddev: 0.0010384182065777845",
            "extra": "mean: 8.86889198484782 msec\nrounds: 66"
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
          "id": "061de58fd0b40083f831b31a8f177f4b93078bce",
          "message": "ci: add security audit workflow\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T12:17:00-08:00",
          "tree_id": "2eff97a264bdc187374d87ccd6a9264ce80fbf1c",
          "url": "https://github.com/AreteDriver/Dossier/commit/061de58fd0b40083f831b31a8f177f4b93078bce"
        },
        "date": 1771445937804,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 218.06053490728726,
            "unit": "iter/sec",
            "range": "stddev: 0.00008092924622498913",
            "extra": "mean: 4.585882541401495 msec\nrounds: 157"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1508.0487046492299,
            "unit": "iter/sec",
            "range": "stddev: 0.00001382115214045117",
            "extra": "mean: 663.1085567177346 usec\nrounds: 1243"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.555341804813294,
            "unit": "iter/sec",
            "range": "stddev: 0.0024538433701777364",
            "extra": "mean: 73.7716550714284 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 35.45763873013646,
            "unit": "iter/sec",
            "range": "stddev: 0.00060327401782158",
            "extra": "mean: 28.20266762857143 msec\nrounds: 35"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 214.7499714307218,
            "unit": "iter/sec",
            "range": "stddev: 0.0011781185796244844",
            "extra": "mean: 4.6565780350876524 msec\nrounds: 171"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 141.71827117474766,
            "unit": "iter/sec",
            "range": "stddev: 0.0010376569209190726",
            "extra": "mean: 7.056253168421285 msec\nrounds: 95"
          }
        ]
      }
    ]
  }
}