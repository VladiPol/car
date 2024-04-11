import os
import importlib

def get_stage() -> str:
    # get environment LOCAL, DEV, INTEG, QSU, PROD etc.
    stage = os.environ.get("STAGE", "LOCAL")
    return stage

def get_config() -> str:
    # load config for environment
    config = importlib.import_module(f"config.config_{get_stage().lower()}")
    return config