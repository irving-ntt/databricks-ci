# tmp_codeql_test.py
import subprocess


def run_user_command(user_input: str) -> None:
    # Vulnerabilidad intencional para prueba: command injection
    subprocess.run(user_input, shell=True, check=False)
