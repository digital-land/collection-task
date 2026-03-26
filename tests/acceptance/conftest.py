import sys
from pathlib import Path

# Allow tests to import scripts from bin/ directly
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "bin"))
