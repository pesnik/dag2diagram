import os
import tiktoken
from dotenv import load_dotenv
from openai import OpenAI
from pathlib import Path
from croniter import croniter
from datetime import datetime
import json

client = OpenAI(base_url="http://127.0.0.1:8181/v1", api_key="dummy")
# client = OpenAI(base_url="http://127.0.0.1:11434/v1", api_key="dummy")


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


def main():
    load_dotenv()

    schedules = get_schedulers()
    print(schedules)

    sequential_dagruns = get_sorted_schedules(schedules)
    print(sequential_dagruns)


if __name__ == "__main__":
    main()
