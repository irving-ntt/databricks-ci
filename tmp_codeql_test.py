import subprocess
import sys


def main() -> None:
    user_input = sys.argv[1]
    subprocess.run(user_input, shell=True, check=False)

if __name__ == "__main__":
    main()
