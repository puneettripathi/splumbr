import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--python_path',
                        help='Provide sedol_list Group to proceed',
                        required=False)

    parser.add_argument('--build_type',
                        default='egg',
                        help='what type of build to perform',
                        required=False)

    parser.add_argument('--setup_py_location',
                        help='Provide location of setup.py',
                        required=False)

    args = parser.parse_args()
    python_path = args.python_path
    build_type = args.build_type
    base_location = os.path.join(os.path.dirname(__file__))
    print(base_location)

    if python_path:
        py_exec = python_path
    else:
        py_exec = base_location + os.sep + "venv" + os.sep + "scripts" + os.sep + "python"

    if args.setup_py_location:
        setup = args.setup_py_location
    else:
        setup = base_location + os.sep + 'setup.py'

    os.system(py_exec + " " + setup + " build")
    os.system(py_exec + " " + setup + " sdist")
    os.system(py_exec + " " + setup + " bdist_egg")
