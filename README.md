# dag2diagram

Convert Airflow DAGs to visual diagrams using local LLMs.

## What It Does

Analyzes your Airflow DAGs incrementally and builds two Mermaid diagrams:
- **Data Lineage**: Table-level data flow (sources → transformations → targets)
- **Business Logic**: Recharge process workflows and decision flows

Uses local LLM (llama.cpp) to understand 25+ DAGs without sending code to external APIs.

## Quick Start
```bash
# 1. Start llama.cpp server
~/Tools/llama.cpp/build/bin/llama-server \
  -m LiquidAI/LFM2-2.6B-GGUF/LFM2-2.6B-Q8_0.gguf \
  --port 8181 \
  --ctx-size 65536 \
  --cache-prompt

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
python dag2diagram.py --dags-dir ./airflow/dags --output ./diagrams
```

## Output

- `lineage.mmd` - Data lineage diagram
- `business.mmd` - Business logic diagram

View with [Mermaid Live Editor](https://mermaid.live) or VS Code.

## Requirements

- Python 3.8+
- llama.cpp server running locally
- 16GB RAM (for 2.6B model with large context)

## Architecture

Incremental learning approach:
1. Read DAG → Extract lineage & logic
2. Merge with existing canvas
3. Repeat for all DAGs
4. Output final diagrams

Handles DAGs from 1k to 33k tokens.

## License

MIT
