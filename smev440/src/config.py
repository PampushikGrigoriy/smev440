"""Configuration module for the application."""

import os
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent

GATE_PATHS = {
    "fns_in": BASE_DIR / os.getenv("GATE_IN_PATH", "fns_in"),
    "fns_out": BASE_DIR / os.getenv("GATE_OUT_PATH", "fns_out"),
    "ens_in": BASE_DIR / os.getenv("ENS_IN_PATH", "ens_in"),
    "ens_out": BASE_DIR / os.getenv("ENS_OUT_PATH", "ens_out"),
}
