import os

from dotenv import dotenv_values


def del_env(dotenv_path='test/.env'):
    for k, v in dotenv_values(dotenv_path=dotenv_path).items():
        del os.environ[k]