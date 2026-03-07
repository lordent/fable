import os
import uuid
from pathlib import Path

def _create_migration_file(migrations_module: str, operations: list):
    folder = Path(*migrations_module.split('.'))
    folder.mkdir(parents=True, exist_ok=True)
    
    existing = sorted([f for f in os.listdir(folder) if f.endswith('.py')])
    idx = int(existing[-1].split('_')[0]) + 1 if existing else 1
    
    uid = uuid.uuid4().hex[:8]
    filename = folder / f"{idx:04d}_{uid}.py"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write("from sql import migration\n\n")
        f.write("def up():\n    return (\n")
        for op in operations:
            f.write(f"        migration.{repr(op)},\n")
        f.write("    )\n")
