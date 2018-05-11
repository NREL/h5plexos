import sys
from .process import process_solution

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    for solutionfile in args:
        process_solution(solutionfile)


if __name__ == "__main__":
    main()
