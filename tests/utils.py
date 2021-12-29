import os
from subprocess import run, PIPE

from dotenv import dotenv_values

import xcube_geodb.version as version_module


def del_env(dotenv_path='test/.env'):
    for k, v in dotenv_values(dotenv_path=dotenv_path).items():
        del os.environ[k]


def get_app_dir():
    import inspect
    # noinspection PyTypeChecker
    version_path = inspect.getfile(version_module)
    return os.path.dirname(version_path)


def make_install_geodb():
    import os
    os.environ["GEODB_VERSION"] = version_module.version
    cwd = os.getcwd()

    app_path = get_app_dir()

    os.chdir(os.path.join(app_path, 'sql'))

    res = run(["make", "release_version"], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    print(res.stdout, res.stderr)

    res = run(["make", "install"], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    print(res.stdout, res.stderr)

    os.chdir(cwd)
