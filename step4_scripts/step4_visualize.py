from __future__ import annotations

import os
from pathlib import Path

MPL_CONFIG_DIR = Path("outputs/.mplconfig")
MPL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))

import matplotlib.pyplot as plt
from rdflib import Graph


TTL_PATH = Path("Data/hi-ontology-populated.ttl")
OUTPUT_DIR = Path("outputs/figures")


QUERIES = {
    "capabilities": """
PREFIX hi: <https://w3id.org/hi-ontology#>
SELECT ?capability (COUNT(DISTINCT ?agent) AS ?agentCount)
WHERE {
  ?agent hi:hasCapability ?capability .
}
GROUP BY ?capability
ORDER BY DESC(?agentCount) ?capability
LIMIT 10
""",
    "team_complexity": """
PREFIX hi: <https://w3id.org/hi-ontology#>
SELECT ?team (COUNT(DISTINCT ?task) AS ?taskCount)
WHERE {
  ?team a hi:HITeam ;
        hi:hasGoal ?goal .
  ?goal hi:requiresTask ?task .
}
GROUP BY ?team
ORDER BY DESC(?taskCount) ?team
LIMIT 10
""",
}


def short_name(value: str) -> str:
    return value.split("#")[-1]


def plot_bar(labels: list[str], values: list[int], title: str, output_path: Path) -> None:
    plt.figure(figsize=(10, 5))
    plt.bar(labels, values, color="#4C78A8")
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200)
    plt.close()


def main() -> None:
    graph = Graph()
    graph.parse(TTL_PATH, format="turtle")

    capability_rows = list(graph.query(QUERIES["capabilities"]))
    capability_labels = [short_name(str(row[0])) for row in capability_rows]
    capability_values = [int(row[1]) for row in capability_rows]
    plot_bar(
        capability_labels,
        capability_values,
        "Top capabilities by number of agents",
        OUTPUT_DIR / "capabilities_per_agent.png",
    )

    team_rows = list(graph.query(QUERIES["team_complexity"]))
    team_labels = [short_name(str(row[0])) for row in team_rows]
    team_values = [int(row[1]) for row in team_rows]
    plot_bar(
        team_labels,
        team_values,
        "Tasks required per team goal structure",
        OUTPUT_DIR / "tasks_per_team.png",
    )

    print(f"Saved figures to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
