# Step 4 Starter

This project file already contains enough material for Section 3 of the report.

## What to compute

- 3 to 5 SPARQL queries that show structure and content in the KG
- simple graph metrics
- an optional small embedding experiment

## Files

- `queries/01_use_cases_overview.rq`
- `queries/02_team_goals_and_tasks.rq`
- `queries/03_capabilities_per_agent.rq`
- `queries/04_task_executions_with_models.rq`
- `queries/05_evaluations_experiments_metrics.rq`
- `scripts/step4_kg_analysis.py`

## Run

Use the Python from your conda environment:

```bash
/Users/bara/opt/anaconda3/envs/knowgraphs/bin/python scripts/step4_kg_analysis.py
```

Optional embedding experiment:

```bash
/Users/bara/opt/anaconda3/envs/knowgraphs/bin/python scripts/step4_kg_analysis.py --run-embedding --epochs 10 --dimension 32
```

## What the metrics mean

- `triples`: total RDF statements
- `resource_edges`: triples whose object is not a literal; useful for graph embeddings
- `distinct_predicates`: number of relation types
- `all_nodes`: all unique RDF terms in the graph
- `resource_nodes`: unique IRIs and blank nodes
- `literal_nodes`: unique literal values

## Good report phrasing

You can describe the metrics like this:

> We first computed basic descriptive statistics of the populated HI knowledge graph, including the total number of RDF triples, the number of unique nodes, and the number of distinct relation types. We additionally exported the resource-to-resource subgraph for a lightweight embedding experiment.

You can describe the query section like this:

> We designed a small SPARQL query set to inspect the graph from complementary perspectives: use case coverage, team-goal-task structure, capability distribution across agents, task execution traces, and evaluation design.
