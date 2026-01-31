import os
import tiktoken
from pathlib import Path
from dotenv import load_dotenv

def main():
    load_dotenv()

    token_count_list = []
    for file_path in Path(os.getenv("FILE_PATH")).rglob("*.py"):
        with open(file_path, 'r') as f:
            enc = tiktoken.get_encoding("cl100k_base")

            tokens = enc.encode(f.read())
            token_count_list.append(len(tokens))

            # print(tokens)
            print(file_path.name)
            print(len(tokens))

    print(sorted(token_count_list))

if __name__ == "__main__":
    main()
