#!/usr/bin/env python3
"""
DOSSIER — CLI Entry Point

Usage:
    python -m dossier serve              # Start web server
    python -m dossier ingest <file>      # Ingest a single file
    python -m dossier ingest-dir <dir>   # Ingest all files in directory
    python -m dossier ingest-emails <dir># Ingest email files (eml, mbox, json, csv)
    python -m dossier search <query>     # Search from CLI
    python -m dossier stats              # Show collection stats
    python -m dossier entities [type]    # List top entities
    python -m dossier init               # Initialize database
    python -m dossier timeline           # Show reconstructed timeline
                                          # --start 2003-01-01 --end 2009-12-31
                                          # --entity "Jane Doe"
    python -m dossier resolve            # Run entity resolution
                                          # --type person  (filter by entity type)
                                          # --dry-run      (show matches without merging)
    python -m dossier graph stats        # Network statistics
    python -m dossier graph centrality   # Top entities by centrality
                                          # --metric degree|betweenness|closeness|eigenvector
                                          # --type person  --limit 20
    python -m dossier graph communities  # Detect communities
                                          # --type person  --min-size 2
    python -m dossier graph path <src> <tgt>  # Shortest path between entities
    python -m dossier graph neighbors <id>    # Entity neighborhood
                                          # --hops 1  --min-weight 1

    # Podesta Email Scrapers
    python -m dossier podesta-download --range 1 100    # Download WikiLeaks emails
    python -m dossier podesta-ingest                    # Ingest downloaded emails
    python -m dossier lobbying --all                    # Download + ingest lobbying records
"""

import sys
import json


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "serve":
        serve()
    elif cmd == "init":
        from dossier.db.database import init_db

        init_db()
    elif cmd == "ingest":
        if len(sys.argv) < 3:
            print("Usage: python -m dossier ingest <filepath> [--source NAME] [--date YYYY-MM-DD]")
            sys.exit(1)
        ingest_cmd()
    elif cmd == "ingest-dir":
        if len(sys.argv) < 3:
            print("Usage: python -m dossier ingest-dir <directory> [--source NAME]")
            sys.exit(1)
        ingest_dir_cmd()
    elif cmd == "ingest-emails":
        if len(sys.argv) < 3:
            print(
                "Usage: python -m dossier ingest-emails <directory> [--source NAME] [--corpus NAME]"
            )
            sys.exit(1)
        ingest_emails_cmd()
    elif cmd == "search":
        if len(sys.argv) < 3:
            print("Usage: python -m dossier search <query>")
            sys.exit(1)
        search_cmd()
    elif cmd == "stats":
        stats_cmd()
    elif cmd == "entities":
        entities_cmd()
    elif cmd == "timeline":
        timeline_cmd()
    elif cmd == "resolve":
        resolve_cmd()
    elif cmd == "podesta-download":
        podesta_download_cmd()
    elif cmd == "podesta-ingest":
        podesta_ingest_cmd()
    elif cmd == "graph":
        graph_cmd()
    elif cmd == "lobbying":
        lobbying_cmd()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


def serve():
    import uvicorn

    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    print("\n  ╔══════════════════════════════════════╗")
    print("  ║  DOSSIER — Document Intelligence     ║")
    print(f"  ║  http://localhost:{port}               ║")
    print(f"  ║  API: http://localhost:{port}/docs      ║")
    print("  ╚══════════════════════════════════════╝\n")
    uvicorn.run("dossier.api.server:app", host="0.0.0.0", port=port, reload=True)


def ingest_cmd():
    from dossier.db.database import init_db
    from dossier.ingestion.pipeline import ingest_file

    init_db()
    filepath = sys.argv[2]

    source = ""
    date = ""
    args = sys.argv[3:]
    for i, arg in enumerate(args):
        if arg == "--source" and i + 1 < len(args):
            source = args[i + 1]
        elif arg == "--date" and i + 1 < len(args):
            date = args[i + 1]

    result = ingest_file(filepath, source=source, date=date)
    if result["success"]:
        print(f"\n✓ Ingested: {filepath}")
        print(f"  Document ID: {result['document_id']}")
        print(f"  Stats: {json.dumps(result['stats'], indent=2)}")
    else:
        print(f"\n✗ Failed: {result['message']}")


def ingest_dir_cmd():
    from dossier.db.database import init_db
    from dossier.ingestion.pipeline import ingest_directory

    init_db()
    dirpath = sys.argv[2]

    source = ""
    args = sys.argv[3:]
    for i, arg in enumerate(args):
        if arg == "--source" and i + 1 < len(args):
            source = args[i + 1]

    results = ingest_directory(dirpath, source=source)
    success = sum(1 for r in results if r["success"])
    failed = len(results) - success

    print(f"\n{'=' * 40}")
    print(f"  Ingested: {success} | Failed: {failed}")
    print(f"{'=' * 40}")


def search_cmd():
    from dossier.db.database import init_db, get_db

    init_db()
    query = " ".join(sys.argv[2:])

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT d.id, d.title, d.category, d.date,
                   snippet(documents_fts, 1, '>>>', '<<<', '...', 30) as excerpt
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE documents_fts MATCH ?
            ORDER BY rank
            LIMIT 20
        """,
            (f'"{query}"',),
        ).fetchall()

    if not rows:
        print(f"No results for: {query}")
        return

    print(f"\n─── Results for: {query} ───\n")
    for row in rows:
        print(f"  [{row['id']:3d}] [{row['category']:15s}] {row['title']}")
        print(f"        {row['date']} | {row['excerpt'][:120]}")
        print()


def stats_cmd():
    from dossier.db.database import init_db, get_db

    init_db()
    with get_db() as conn:
        docs = conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]
        entities = conn.execute("SELECT COUNT(*) as c FROM entities").fetchone()["c"]
        pages = conn.execute("SELECT COALESCE(SUM(pages), 0) as c FROM documents").fetchone()["c"]
        keywords = conn.execute("SELECT COUNT(*) as c FROM keywords").fetchone()["c"]

        cats = conn.execute(
            "SELECT category, COUNT(*) as c FROM documents GROUP BY category ORDER BY c DESC"
        ).fetchall()

    print("\n  DOSSIER — Collection Stats")
    print(f"  {'─' * 30}")
    print(f"  Documents:  {docs}")
    print(f"  Entities:   {entities}")
    print(f"  Pages:      {pages}")
    print(f"  Keywords:   {keywords}")
    print("\n  Categories:")
    for row in cats:
        print(f"    {row['category']:20s} {row['c']}")
    print()


def entities_cmd():
    from dossier.db.database import init_db, get_db

    init_db()
    etype = sys.argv[2] if len(sys.argv) > 2 else None

    with get_db() as conn:
        sql = """
            SELECT e.name, e.type, SUM(de.count) as total
            FROM entities e
            JOIN document_entities de ON de.entity_id = e.id
        """
        params = []
        if etype:
            sql += " WHERE e.type = ?"
            params.append(etype)
        sql += " GROUP BY e.id ORDER BY total DESC LIMIT 30"

        rows = conn.execute(sql, params).fetchall()

    if not rows:
        print("No entities found.")
        return

    print(f"\n  Top Entities{' (' + etype + ')' if etype else ''}:")
    print(f"  {'─' * 40}")
    for row in rows:
        print(f"  {row['total']:6d}  [{row['type']:6s}]  {row['name']}")
    print()


def timeline_cmd():
    from dossier.db.database import init_db, get_db
    from dossier.forensics.timeline import query_timeline

    init_db()

    start = None
    end = None
    entity = None
    args = sys.argv[2:]
    for i, arg in enumerate(args):
        if arg == "--start" and i + 1 < len(args):
            start = args[i + 1]
        elif arg == "--end" and i + 1 < len(args):
            end = args[i + 1]
        elif arg == "--entity" and i + 1 < len(args):
            entity = args[i + 1]

    with get_db() as conn:
        events = query_timeline(conn, start_date=start, end_date=end, entity_name=entity, limit=50)

    if not events:
        print("No timeline events found.")
        return

    print(f"\n─── Timeline ({len(events)} events) ───\n")
    for ev in events:
        date_str = ev["event_date"] or "(unresolved)"
        precision = ev["precision"]
        confidence = ev["confidence"]
        context = ev["context"][:120]
        entities_list = [e["name"] for e in ev.get("entities", [])]
        ent_str = f"  [{', '.join(entities_list)}]" if entities_list else ""
        print(f"  {date_str:12s}  [{precision:6s}] ({confidence:.0%})  {context}")
        if ent_str:
            print(f"               {ent_str}")
    print()


def resolve_cmd():
    from dossier.db.database import init_db, get_db
    from dossier.core.resolver import EntityResolver

    init_db()

    entity_type = None
    dry_run = False
    args = sys.argv[2:]
    for i, arg in enumerate(args):
        if arg == "--type" and i + 1 < len(args):
            entity_type = args[i + 1]
        elif arg == "--dry-run":
            dry_run = True

    with get_db() as conn:
        resolver = EntityResolver(conn)

        if dry_run:
            # Show candidates without merging
            entities = conn.execute(
                "SELECT id, name, type FROM entities" + (" WHERE type = ?" if entity_type else ""),
                [entity_type] if entity_type else [],
            ).fetchall()

            print(f"\n─── Dry Run: Scanning {len(entities)} entities ───\n")
            total_candidates = 0
            for entity in entities:
                matches = resolver.resolve_entity(entity["id"])
                for m in matches:
                    total_candidates += 1
                    print(
                        f"  {m.source_name:30s} → {m.target_name:30s}  "
                        f"({m.confidence:.0%} {m.strategy}) [{m.action.value}]"
                    )

            if total_candidates == 0:
                print("  No candidates found.")
            print(f"\n  Total candidates: {total_candidates}")
        else:
            result = resolver.resolve_all(entity_type=entity_type)
            print("\n  DOSSIER — Entity Resolution")
            print(f"  {'─' * 30}")
            print(f"  Entities scanned:  {result.entities_scanned}")
            print(f"  Auto-merged:       {result.auto_merged}")
            print(f"  Suggested (queue): {result.suggested}")
            print()

            if result.matches:
                print("  Matches:")
                for m in result.matches:
                    print(
                        f"    {m.source_name:30s} → {m.target_name:30s}  "
                        f"({m.confidence:.0%} {m.strategy})"
                    )
                print()

    print()


def graph_cmd():
    from dossier.db.database import init_db, get_db
    from dossier.core.graph_analysis import GraphAnalyzer

    init_db()

    args = sys.argv[2:]
    if not args:
        print("Usage: python -m dossier graph <stats|centrality|communities|path|neighbors>")
        sys.exit(1)

    subcmd = args[0]
    sub_args = args[1:]

    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)

        if subcmd == "stats":
            entity_type = None
            for i, arg in enumerate(sub_args):
                if arg == "--type" and i + 1 < len(sub_args):
                    entity_type = sub_args[i + 1]
            stats = analyzer.get_stats(entity_type=entity_type)
            print("\n  DOSSIER — Network Stats")
            print(f"  {'─' * 30}")
            print(f"  Nodes:              {stats.node_count}")
            print(f"  Edges:              {stats.edge_count}")
            print(f"  Density:            {stats.density:.4f}")
            print(f"  Components:         {stats.components}")
            print(f"  Avg degree:         {stats.avg_degree:.2f}")
            print(f"  Avg weighted degree: {stats.avg_weighted_degree:.2f}")
            print()

        elif subcmd == "centrality":
            metric = "degree"
            entity_type = None
            limit = 20
            for i, arg in enumerate(sub_args):
                if arg == "--metric" and i + 1 < len(sub_args):
                    metric = sub_args[i + 1]
                elif arg == "--type" and i + 1 < len(sub_args):
                    entity_type = sub_args[i + 1]
                elif arg == "--limit" and i + 1 < len(sub_args):
                    limit = int(sub_args[i + 1])
            results = analyzer.get_centrality(metric=metric, entity_type=entity_type, limit=limit)
            if not results:
                print("No entities found.")
                return
            print(f"\n  Top {len(results)} by {metric} centrality:")
            print(f"  {'─' * 50}")
            for r in results:
                score = getattr(r, metric)
                print(f"  {score:8.4f}  [{r.type:6s}]  {r.name}")
            print()

        elif subcmd == "communities":
            entity_type = None
            min_size = 2
            for i, arg in enumerate(sub_args):
                if arg == "--type" and i + 1 < len(sub_args):
                    entity_type = sub_args[i + 1]
                elif arg == "--min-size" and i + 1 < len(sub_args):
                    min_size = int(sub_args[i + 1])
            communities = analyzer.get_communities(entity_type=entity_type, min_size=min_size)
            if not communities:
                print("No communities found.")
                return
            print(f"\n  Detected {len(communities)} communities:")
            print(f"  {'─' * 50}")
            for c in communities:
                names = [m["name"] for m in c.members[:5]]
                extra = f" +{c.size - 5} more" if c.size > 5 else ""
                print(f"  Community {c.id} ({c.size} members, density={c.density:.2f}):")
                print(f"    {', '.join(names)}{extra}")
            print()

        elif subcmd == "path":
            if len(sub_args) < 2:
                print("Usage: python -m dossier graph path <source_id> <target_id>")
                sys.exit(1)
            source_id = int(sub_args[0])
            target_id = int(sub_args[1])
            result = analyzer.find_shortest_path(source_id, target_id)
            if result is None:
                print("No path found.")
                return
            print(f"\n  Path ({result.hops} hops, total weight {result.total_weight}):")
            print(f"  {'─' * 50}")
            for i, node in enumerate(result.nodes):
                print(f"  {node['name']} [{node['type']}]")
                if i < len(result.edges):
                    print(f"    -- weight {result.edges[i]['weight']} -->")
            print()

        elif subcmd == "neighbors":
            if not sub_args:
                print(
                    "Usage: python -m dossier graph neighbors <entity_id> [--hops N] [--min-weight N]"
                )
                sys.exit(1)
            entity_id = int(sub_args[0])
            hops = 1
            min_weight = 1
            for i, arg in enumerate(sub_args[1:]):
                if arg == "--hops" and i + 2 < len(sub_args):
                    hops = int(sub_args[i + 2])
                elif arg == "--min-weight" and i + 2 < len(sub_args):
                    min_weight = int(sub_args[i + 2])
            neighbors = analyzer.get_neighbors(entity_id, hops=hops, min_weight=min_weight)
            if not neighbors:
                print("No neighbors found.")
                return
            print(f"\n  Neighbors of entity {entity_id} ({len(neighbors)} found):")
            print(f"  {'─' * 50}")
            for n in neighbors:
                print(f"  hop {n['hop']}  weight {n['weight']:3d}  [{n['type']:6s}]  {n['name']}")
            print()

        else:
            print(f"Unknown graph subcommand: {subcmd}")
            print("Usage: python -m dossier graph <stats|centrality|communities|path|neighbors>")
            sys.exit(1)


def ingest_emails_cmd():
    from dossier.db.database import init_db
    from dossier.ingestion.email_pipeline import ingest_email_directory

    init_db()
    dirpath = sys.argv[2]
    source = ""
    corpus = ""
    args = sys.argv[3:]
    for i, arg in enumerate(args):
        if arg == "--source" and i + 1 < len(args):
            source = args[i + 1]
        elif arg == "--corpus" and i + 1 < len(args):
            corpus = args[i + 1]

    result = ingest_email_directory(dirpath, source=source, corpus=corpus)
    print(f"\n{'=' * 40}")
    print(f"  Ingested: {result['ingested']} | Failed: {result['failed']}")
    print(f"{'=' * 40}")


def podesta_download_cmd():
    """Handle podesta-download command."""
    args = sys.argv[2:]

    start = 1
    end = 100
    delay = 1.5

    for i, arg in enumerate(args):
        if arg == "--range" and i + 2 < len(args):
            start = int(args[i + 1])
            end = int(args[i + 2])
        elif arg == "--delay" and i + 1 < len(args):
            delay = float(args[i + 1])

    from dossier.ingestion.scrapers.wikileaks_podesta import download_range

    download_range(start, end, delay=delay)


def podesta_ingest_cmd():
    """Ingest downloaded Podesta emails."""
    limit = 0
    args = sys.argv[2:]
    for i, arg in enumerate(args):
        if arg == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])

    from dossier.ingestion.scrapers.wikileaks_podesta import ingest_downloaded
    from dossier.db.database import init_db

    init_db()
    ingest_downloaded(limit=limit)


def lobbying_cmd():
    """Handle lobbying command."""
    args = sys.argv[2:]

    if "--all" in args:
        from dossier.ingestion.scrapers.fara_lobbying import (
            create_lobbying_index,
            generate_ingestable_documents,
            ingest_lobbying_docs,
        )

        create_lobbying_index()
        generate_ingestable_documents()
        ingest_lobbying_docs()
    elif "--create-index" in args:
        from dossier.ingestion.scrapers.fara_lobbying import create_lobbying_index

        create_lobbying_index()
    elif "--generate-docs" in args:
        from dossier.ingestion.scrapers.fara_lobbying import generate_ingestable_documents

        generate_ingestable_documents()
    elif "--ingest" in args:
        from dossier.ingestion.scrapers.fara_lobbying import ingest_lobbying_docs

        ingest_lobbying_docs()
    else:
        print("Usage: python -m dossier lobbying [--all|--create-index|--generate-docs|--ingest]")


if __name__ == "__main__":
    main()
