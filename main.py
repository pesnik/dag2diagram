import os
import tiktoken
from dotenv import load_dotenv
from openai import OpenAI
from pathlib import Path
from croniter import croniter
from datetime import datetime
import json
from mock import schedulers as mock_data

client = OpenAI(base_url="http://127.0.0.1:8181/v1", api_key="dummy")
client = OpenAI(base_url="http://127.0.0.1:11434/v1", api_key="dummy")


def get_schedulers():
    system_prompt = """Extract DAG schedule. Rules:
        1. Find schedule_interval or schedule in DAG()
        2. Ignore lines starting with #
        3. Output ONLY: schedule="VALUE"
        4. If not found: schedule="None"
        5. NO explanations, NO json, NO markdown

        VALID outputs:
            schedule="@daily"
            schedule="0 2 * * *"
            schedule="None"

            INVALID outputs:
            ```json...
            To extract...
            The provided code..."""

    schedules = []
    for file_path in Path(os.getenv("FILE_PATH")).rglob("*.py"):
        with open(file_path, "r") as f:
            dag_code = f.read()

            enc = tiktoken.get_encoding("cl100k_base")

            tokens = len(enc.encode(dag_code))
            print(f"{file_path.name:<50} : {tokens:>6}")

            if tokens == 0:
                continue

            schedule_lines = [
                line for line in dag_code.split("\n") if "schedule" in line.lower()
            ]

            messages = [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": f"""TASK: Extract the active schedule from this DAG.

                    DAG LINES CONTAINING 'schedule':
                    {chr(10).join(schedule_lines)}

                    REMEMBER: Output ONLY this format: schedule="VALUE"
                    Find schedule_interval or schedule parameter. Ignore commented lines (starting with #).

                    Your response:""",
                },
            ]

            chat_completion = client.chat.completions.create(
                messages=messages,
                model="LFM2-2.6B-Q8_0.gguf",
                # model="qwen2.5-coder-7b-q8_0.gguf",
                # model="qwen2.5-coder:7b",
                # model="qwen2.5-coder-1.5b-q8_0.gguf",
                # temperature=0.0,
                # max_tokens=50,
            )
            print(chat_completion.choices[0].message.content)
            schedule_assignment = chat_completion.choices[0].message.content
            schedules.append((file_path.name, schedule_assignment))

    return schedules


def get_sorted_schedules(schedules):
    def get_execution_time(schedule_tuple):
        dag, schedule_str = schedule_tuple
        print(dag)

        cron = schedule_str.split('schedule="')[1].rstrip('"')

        base = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            iter = croniter(cron, base)
            return iter.get_next(datetime)
        except Exception as e:
            print(f"Error parsing {dag}: {cron} - {e}")
            return datetime.max

    sorted_schedules = sorted(schedules, key=get_execution_time)

    print("\nSorted Execution Order:")
    print("=" * 80)
    for dag, schedule in sorted_schedules:
        cron = schedule.split('schedule="')[1].rstrip('"')
        exec_time = get_execution_time((dag, schedule))
        print(f"{exec_time.strftime('%H:%M')} | {dag}")
    print("=" * 80)

    return [dag for dag, _ in sorted_schedules]


def build_mermaid_from_pairs(all_pairs, output_file="lineage_canvas.mmd"):
    """Convert collected pairs to Mermaid diagram"""
    
    mermaid_lines = ["graph LR"]
    
    # Track unique nodes and edges
    nodes = set()
    edges = []
    
    for dag_file, pairs in all_pairs.items():
        for source, target in pairs:
            if source:
                nodes.add(source)
            if target:
                nodes.add(target)
            
            if source and target:
                # Normal edge with DAG label
                dag_name = dag_file.replace('.py', '')
                edges.append(f'    {source}[("{source}")] -->|{dag_name}| {target}[("{target}")]')
            elif source and not target:
                # Source only (extract/read)
                edges.append(f'    {source}[("{source}")]')
            elif target and not source:
                # Target only (load/write from unknown source)
                edges.append(f'    {target}[("{target}")]')
    
    # Write Mermaid
    mermaid_lines.extend(edges)
    
    mermaid_content = "\n".join(mermaid_lines)
    
    with open(output_file, "w") as f:
        f.write(mermaid_content)
    
    print(f"\n{'='*80}")
    print(f"Mermaid diagram saved to: {output_file}")
    print(f"Total nodes: {len(nodes)}")
    print(f"Total edges: {len(edges)}")
    print(f"{'='*80}")
    
    return mermaid_content

def get_extracted_lineage(dag_filename):
    system_prompt = """Extract data lineage from this Airflow DAG as source-target pairs.

Look for:
- SQL queries: SELECT FROM (source) and INSERT INTO (target)
- Table operators: read_table → write_table
- Dataset transformations

OUTPUT: JSON array of [source, target] pairs
Format: [["source_table", "target_table"], ["table_a", "table_b"]]

Rules:
- Use full table names with schema if present (schema.table)
- If table is both read and written in same query, include as pair
- If only reading (no write), use null as target: ["source", null]
- If only writing (no read), use null as source: [null, "target"]

Example output:
[
  ["stg_recharge.raw_data", "dwh_recharge.processed"],
  ["dwh_recharge.processed", "dwh_recharge.summary"],
  ["external_api", "stg_recharge.raw_data"]
]

No explanations. Only JSON array."""

    with open(os.getenv("FILE_PATH") + dag_filename, "r") as f:
        dag_code = f.read()
        enc = tiktoken.get_encoding("cl100k_base")
        tokens = len(enc.encode(dag_code))
        print(f"{dag_filename:<60} : {tokens:>6} tokens", end="")
        
        if tokens > 10000:
            print(" [SKIPPED - too large]")
            return []
        
        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user", 
                "content": f"""DAG filename: {dag_filename}

Code:
{dag_code}

Extract source-target pairs as JSON array:"""
            }
        ]
        
        chat_completion = client.chat.completions.create(
            messages=messages,
            model="qwen2.5-coder:7b",
			# model="LFM2-2.6B-Q8_0.gguf",
            temperature=0.0,
            max_tokens=1000,
            stop=["Example", "Rules", "\n\n\n"]
        )
        
        response = chat_completion.choices[0].message.content.strip()
        print(f" [EXTRACTED]")
        
        import re
        import json
        
        try:
            json_match = re.search(r'\[\s*\[.*?\]\s*\]', response, re.DOTALL)
            if json_match:
                pairs = json.loads(json_match.group())
                
                # Display pairs
                print(f"{'':60}   Found {len(pairs)} pairs:")
                for source, target in pairs:
                    arrow = "→" if source and target else "○"
                    src = source or "null"
                    tgt = target or "null"
                    print(f"{'':60}     {src} {arrow} {tgt}")
                
                return pairs
            else:
                print(f"{'':60}   [WARNING: No JSON found in response]")
                print(f"{'':60}   Response: {response[:200]}")
                return []
                
        except Exception as e:
            print(f"{'':60}   [ERROR: {e}]")
            return []

def paint(all_lineage):
    print(f"\n{'='*80}")
    print("BUILDING MERMAID DIAGRAM")
    print(f"{'='*80}\n")
    
    mermaid = build_mermaid_from_pairs(all_lineage)
    
    print("\nPreview:")
    print(mermaid[:500])
    print("...")

def main():
    load_dotenv()

    # schedules = get_schedulers()
    schedules = mock_data

    sorted_dags = get_sorted_schedules(schedules)
    all_lineage = {}

    print(f"\n{'='*80}")
    print("EXTRACTING DATA LINEAGE")
    print(f"{'='*80}\n")
    
    for dag_file in sorted_dags:
        pairs = get_extracted_lineage(dag_file)
        if pairs:
            all_lineage[dag_file] = pairs

	paint(all_lineage)

if __name__ == "__main__":
    main()
