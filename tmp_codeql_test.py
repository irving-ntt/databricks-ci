"""
Archivo de prueba intencionalmente vulnerable para validar CodeQL.

Este archivo ejecuta entrada controlada por el usuario con `shell=True`,
lo que debería ser detectado por las queries de CodeQL que buscan
inyección de comandos/uso inseguro de `subprocess`.
"""
import sys
import subprocess


def dangerous(user_input: str) -> None:
    # Intencionalmente vulnerable: ejecutar entrada externa con shell=True
    subprocess.run(user_input, shell=True, check=False)


def main() -> None:
    if len(sys.argv) > 1:
        dangerous(sys.argv[1])
    else:
        print("Usage: python tmp_codeql_vuln.py '<command>'")


if __name__ == "__main__":
    main()
