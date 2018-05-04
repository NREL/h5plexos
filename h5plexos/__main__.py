import sys
from buildh5 import process_solution

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    for solution in solutions:
        process_solution(solution)


if __name__ == "__main__":
    main()
