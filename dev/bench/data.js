window.BENCHMARK_DATA = {
  "lastUpdate": 1771619850116,
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
          "id": "596563ae0fa76f63a74bf3258bb4b3bc29f6a858",
          "message": "ci: add gitleaks secret scanning\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T12:20:45-08:00",
          "tree_id": "7a783f3678435051420773b989ef60456d0ff9a9",
          "url": "https://github.com/AreteDriver/Dossier/commit/596563ae0fa76f63a74bf3258bb4b3bc29f6a858"
        },
        "date": 1771446152119,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 243.3316131163858,
            "unit": "iter/sec",
            "range": "stddev: 0.000027932962345221096",
            "extra": "mean: 4.109618093567229 msec\nrounds: 171"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1800.1914521925373,
            "unit": "iter/sec",
            "range": "stddev: 0.000023459208268168635",
            "extra": "mean: 555.4964716569747 usec\nrounds: 1376"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.67608059511642,
            "unit": "iter/sec",
            "range": "stddev: 0.0010135440041698475",
            "extra": "mean: 73.12036464285602 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 39.239010660544274,
            "unit": "iter/sec",
            "range": "stddev: 0.00011044355042125817",
            "extra": "mean: 25.48484233333444 msec\nrounds: 39"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 232.9171379988237,
            "unit": "iter/sec",
            "range": "stddev: 0.0012310316748490012",
            "extra": "mean: 4.293372349462109 msec\nrounds: 186"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 162.3789741816403,
            "unit": "iter/sec",
            "range": "stddev: 0.00038055088402759",
            "extra": "mean: 6.158432796116697 msec\nrounds: 103"
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
          "id": "33c6cb6a2a4e82719171376d670d034c89256dc5",
          "message": "chore: add gitleaks configuration\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-20T01:15:46-08:00",
          "tree_id": "cc9aa4b6522c53f944e3c1b3f905223fd8753024",
          "url": "https://github.com/AreteDriver/Dossier/commit/33c6cb6a2a4e82719171376d670d034c89256dc5"
        },
        "date": 1771579041398,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 209.9219688067116,
            "unit": "iter/sec",
            "range": "stddev: 0.0003337014609161651",
            "extra": "mean: 4.763674834436996 msec\nrounds: 151"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1536.8636473492215,
            "unit": "iter/sec",
            "range": "stddev: 0.000017592931073510768",
            "extra": "mean: 650.6758109119163 usec\nrounds: 1338"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.484692924029712,
            "unit": "iter/sec",
            "range": "stddev: 0.0003781729635399973",
            "extra": "mean: 74.15815885714393 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 35.44649389117974,
            "unit": "iter/sec",
            "range": "stddev: 0.00029997843871048743",
            "extra": "mean: 28.211534914284798 msec\nrounds: 35"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 213.04228568451148,
            "unit": "iter/sec",
            "range": "stddev: 0.0011175398455975983",
            "extra": "mean: 4.693903826589961 msec\nrounds: 173"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 136.6090613162904,
            "unit": "iter/sec",
            "range": "stddev: 0.000593505664433097",
            "extra": "mean: 7.320158636363836 msec\nrounds: 88"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "committer": {
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "id": "af90542591d6c8ecfb2cec03bd2002f8902b58d2",
          "message": "chore(deps): bump actions/checkout from 4 to 6",
          "timestamp": "2026-02-20T09:15:51Z",
          "url": "https://github.com/AreteDriver/Dossier/pull/4/commits/af90542591d6c8ecfb2cec03bd2002f8902b58d2"
        },
        "date": 1771619839131,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 217.27290776147487,
            "unit": "iter/sec",
            "range": "stddev: 0.00003648270226729927",
            "extra": "mean: 4.602506636942575 msec\nrounds: 157"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1506.1840407384154,
            "unit": "iter/sec",
            "range": "stddev: 0.00001331577886456639",
            "extra": "mean: 663.9294886631147 usec\nrounds: 1279"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.602113307490615,
            "unit": "iter/sec",
            "range": "stddev: 0.0001664887871738738",
            "extra": "mean: 73.51798778571451 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 35.9932649042785,
            "unit": "iter/sec",
            "range": "stddev: 0.00013149356385542193",
            "extra": "mean: 27.78297558333283 msec\nrounds: 36"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 212.93873668492205,
            "unit": "iter/sec",
            "range": "stddev: 0.001039132967980906",
            "extra": "mean: 4.696186403508464 msec\nrounds: 171"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 138.11332420994626,
            "unit": "iter/sec",
            "range": "stddev: 0.0006271126082219101",
            "extra": "mean: 7.2404310425538565 msec\nrounds: 94"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "committer": {
            "name": "AreteDriver",
            "username": "AreteDriver"
          },
          "id": "e4fa304fd61ef0ded651946ddf22ba64e8fb5f68",
          "message": "chore(deps): bump actions/setup-python from 5 to 6",
          "timestamp": "2026-02-20T09:15:51Z",
          "url": "https://github.com/AreteDriver/Dossier/pull/5/commits/e4fa304fd61ef0ded651946ddf22ba64e8fb5f68"
        },
        "date": 1771619849852,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/test_benchmarks.py::TestNERBenchmark::test_extract_entities_5k",
            "value": 215.98766181143955,
            "unit": "iter/sec",
            "range": "stddev: 0.00009197799627762004",
            "extra": "mean: 4.6298940949368435 msec\nrounds: 158"
          },
          {
            "name": "tests/test_benchmarks.py::TestFTS5Benchmark::test_fts5_search_500_docs",
            "value": 1533.9301286150164,
            "unit": "iter/sec",
            "range": "stddev: 0.000015101191598514464",
            "extra": "mean: 651.9201763791541 usec\nrounds: 1287"
          },
          {
            "name": "tests/test_benchmarks.py::TestResolverBenchmark::test_resolve_all_200_entities",
            "value": 13.607923553297962,
            "unit": "iter/sec",
            "range": "stddev: 0.00021067519173443563",
            "extra": "mean: 73.48659742857271 msec\nrounds: 14"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_centrality_betweenness_100",
            "value": 35.67275158987018,
            "unit": "iter/sec",
            "range": "stddev: 0.00021419060499834166",
            "extra": "mean: 28.032600666665846 msec\nrounds: 36"
          },
          {
            "name": "tests/test_benchmarks.py::TestGraphBenchmark::test_communities_100",
            "value": 213.9448871975174,
            "unit": "iter/sec",
            "range": "stddev: 0.0010030511686434157",
            "extra": "mean: 4.674100947674359 msec\nrounds: 172"
          },
          {
            "name": "tests/test_benchmarks.py::TestBulkInsertBenchmark::test_bulk_insert_100_docs",
            "value": 121.71665663724147,
            "unit": "iter/sec",
            "range": "stddev: 0.0007683220125880466",
            "extra": "mean: 8.21580240229858 msec\nrounds: 87"
          }
        ]
      }
    ]
  }
}