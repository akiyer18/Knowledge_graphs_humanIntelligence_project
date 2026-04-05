from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path

import numpy as np
from rdflib import BNode, Graph, Literal, URIRef


REPO_ROOT = Path(__file__).resolve().parents[1]
QUERY_DIR = REPO_ROOT / "step4_queries"
DEFAULT_TTL = REPO_ROOT / "Data/hi-ontology-populated.ttl"
DEFAULT_EDGES = REPO_ROOT / "step4_outputs" / "resource_edges.tsv"
HI_NAMESPACE = "https://w3id.org/hi-ontology#"
OWL_NAMESPACE = "http://www.w3.org/2002/07/owl#"


def shorten_term(term: URIRef | BNode | Literal) -> str:
    if isinstance(term, BNode):
        return f"_:{term}"

    text = str(term)
    if text.startswith(HI_NAMESPACE):
        return f"hi:{text.removeprefix(HI_NAMESPACE)}"
    if text.startswith(OWL_NAMESPACE):
        return f"owl:{text.removeprefix(OWL_NAMESPACE)}"
    return text


def term_namespace(term: URIRef | BNode | Literal) -> str | None:
    if isinstance(term, (Literal, BNode)):
        return None

    text = str(term)
    if "#" in text:
        return text.rsplit("#", 1)[0] + "#"
    if "/" in text:
        return text.rsplit("/", 1)[0] + "/"
    return text


def load_graph(ttl_path: Path) -> Graph:
    graph = Graph()
    graph.parse(ttl_path, format="turtle")
    return graph


def compute_metrics(graph: Graph) -> dict[str, int]:
    all_terms = set()
    resource_terms = set()
    literal_terms = set()
    predicate_terms = set()
    resource_edges = 0

    for s, p, o in graph:
        all_terms.update((s, p, o))
        predicate_terms.add(p)

        if not isinstance(o, Literal):
            resource_edges += 1

        for term in (s, p, o):
            if isinstance(term, Literal):
                literal_terms.add(term)
            else:
                resource_terms.add(term)

    return {
        "triples": len(graph),
        "resource_edges": resource_edges,
        "distinct_predicates": len(predicate_terms),
        "all_nodes": len(all_terms),
        "resource_nodes": len(resource_terms),
        "literal_nodes": len(literal_terms),
    }


def top_predicates(graph: Graph, limit: int = 10) -> list[tuple[str, int]]:
    counts = Counter(str(predicate) for _, predicate, _ in graph)
    return [(shorten_term(URIRef(predicate)), count) for predicate, count in counts.most_common(limit)]


def top_namespaces(graph: Graph, limit: int = 8) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for s, p, o in graph:
        for term in (s, p, o):
            namespace = term_namespace(term)
            if namespace is not None:
                counts[namespace] += 1
    return counts.most_common(limit)


def top_hi_classes(graph: Graph, limit: int = 12) -> list[tuple[str, int]]:
    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?class (COUNT(DISTINCT ?instance) AS ?count)
    WHERE {{
      ?instance rdf:type ?class .
      FILTER(STRSTARTS(STR(?class), "{HI_NAMESPACE}"))
    }}
    GROUP BY ?class
    ORDER BY DESC(?count) ?class
    LIMIT {limit}
    """
    return [(shorten_term(row[0]), int(row[1])) for row in graph.query(query)]


def print_profile_table(title: str, rows: list[tuple[str, int]], formatter=str) -> None:
    print(f"\n{title}")
    if not rows:
        print("- no data")
        return

    for label, value in rows:
        print(f"- {formatter(label)}: {value}")


def print_metrics(metrics: dict[str, int]) -> None:
    print("KG metrics")
    for key, value in metrics.items():
        print(f"- {key}: {value}")


def run_query(graph: Graph, query_path: Path, limit: int) -> None:
    print(f"\nQuery: {query_path.name}")
    results = graph.query(query_path.read_text())
    variables = [str(var) for var in results.vars]
    print(" | ".join(variables))

    for index, row in enumerate(results):
        if index >= limit:
            print(f"... truncated after {limit} rows")
            break
        print(" | ".join("" if value is None else str(value) for value in row))


def export_resource_edges(graph: Graph, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["head", "relation", "tail"])
        for s, p, o in graph:
            if isinstance(o, Literal):
                continue
            writer.writerow([str(s), str(p), str(o)])
            count += 1

    return count


def run_embedding_experiment(edge_path: Path, epochs: int, dimension: int) -> None:
    from pykeen.pipeline import pipeline
    from pykeen.triples import TriplesFactory

    triples: list[tuple[str, str, str]] = []
    with edge_path.open("r", encoding="utf-8") as handle:
        next(handle)
        for line in handle:
            head, relation, tail = line.rstrip("\n").split("\t")
            triples.append((head, relation, tail))

    triples_array = np.asarray(triples, dtype=str)
    triples_factory = TriplesFactory.from_labeled_triples(triples_array)
    training, testing, validation = triples_factory.split(
        [0.8, 0.1, 0.1],
        random_state=42,
    )

    result = pipeline(
        training=training,
        testing=testing,
        validation=validation,
        model="TransE",
        model_kwargs={"embedding_dim": dimension},
        training_kwargs={"num_epochs": epochs, "batch_size": 256},
        random_seed=42,
        evaluator_kwargs={"filtered": True},
        device="cpu",
    )

    metrics = result.metric_results.to_dict()
    both_realistic = metrics.get("both", {}).get("realistic", {})
    best_candidates = result.model

    print("\nEmbedding experiment")
    print(f"- model: TransE")
    print(f"- embedding_dim: {dimension}")
    print(f"- epochs: {epochs}")
    print(f"- training triples: {training.num_triples}")
    print(f"- testing triples: {testing.num_triples}")
    print(f"- validation triples: {validation.num_triples}")
    if both_realistic:
        for metric_name in ("inverse_harmonic_mean_rank", "hits_at_1", "hits_at_3", "hits_at_10"):
            if metric_name in both_realistic:
                print(f"- {metric_name}: {both_realistic[metric_name]}")
    else:
        print("- evaluation metrics available in result.metric_results.to_dict()")
    if best_candidates is not None:
        print("- learned embeddings: yes")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 4 KG starter analysis.")
    parser.add_argument("--ttl", type=Path, default=DEFAULT_TTL, help="Path to the Turtle KG.")
    parser.add_argument(
        "--query-dir",
        type=Path,
        default=QUERY_DIR,
        help="Directory containing .rq query files.",
    )
    parser.add_argument(
        "--query-limit",
        type=int,
        default=10,
        help="Maximum number of rows to print per query.",
    )
    parser.add_argument(
        "--export-edges",
        type=Path,
        default=DEFAULT_EDGES,
        help="Where to export resource-to-resource triples as TSV.",
    )
    parser.add_argument(
        "--run-embedding",
        action="store_true",
        help="Run a small TransE experiment with PyKEEN.",
    )
    parser.add_argument("--epochs", type=int, default=10, help="Embedding epochs.")
    parser.add_argument("--dimension", type=int, default=32, help="Embedding dimension.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    graph = load_graph(args.ttl)

    metrics = compute_metrics(graph)
    print_metrics(metrics)
    print_profile_table("Top namespaces", top_namespaces(graph))
    print_profile_table("Top predicates", top_predicates(graph))
    print_profile_table("Top HI classes by instance count", top_hi_classes(graph))

    if args.query_dir.exists():
        for query_path in sorted(args.query_dir.glob("*.rq")):
            run_query(graph, query_path, args.query_limit)
    else:
        print(f"\nQuery directory not found: {args.query_dir}")

    exported = export_resource_edges(graph, args.export_edges)
    print(f"\nExported {exported} resource edges to {args.export_edges}")

    if args.run_embedding:
        run_embedding_experiment(args.export_edges, args.epochs, args.dimension)


if __name__ == "__main__":
    main()
