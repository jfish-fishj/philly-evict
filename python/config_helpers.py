"""
config_helpers.py

Shared config loading utilities for Python scripts.
Mirrors the semantics of R's r/config.R (read_config, p_product, p_input, etc.).
"""

import os
from pathlib import Path
from typing import Optional

import yaml


def load_config(config_path: Optional[str] = None) -> dict:
    """Load config.yml, mimicking R's read_config() behavior."""
    if config_path is None:
        config_path = os.environ.get("PHILLY_EVICTIONS_CONFIG")
    if config_path is None:
        if Path("config.yml").exists():
            config_path = "config.yml"
        elif Path("config.example.yml").exists():
            config_path = "config.example.yml"
        else:
            raise FileNotFoundError("No config.yml or config.example.yml found")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    cfg["_config_path"] = config_path
    return cfg


def _repo_root(cfg: dict) -> Path:
    return Path(cfg["paths"].get("repo_root", "."))


def p_product(cfg: dict, key: str) -> str:
    """Full path to a product file (relative to processed_dir)."""
    processed_dir = cfg["paths"].get("processed_dir", "data/processed")
    product_path = cfg["products"].get(key)
    if product_path is None:
        raise KeyError(f"Product '{key}' not found in config")
    return str(_repo_root(cfg) / processed_dir / product_path)


def p_input(cfg: dict, key: str) -> str:
    """Full path to an input file (relative to input_dir)."""
    input_dir = cfg["paths"].get("input_dir", "data/inputs")
    input_path = cfg["inputs"].get(key)
    if input_path is None:
        raise KeyError(f"Input '{key}' not found in config")
    return str(_repo_root(cfg) / input_dir / input_path)


def p_output(cfg: dict, *parts: str) -> str:
    """Full path to an output file (relative to output_dir)."""
    output_dir = cfg["paths"].get("output_dir", "output")
    return str(_repo_root(cfg) / output_dir / Path(*parts))


def p_tmp(cfg: dict, *parts: str) -> str:
    """Full path to a temp/scratch file (relative to tmp_dir)."""
    tmp_dir = cfg["paths"].get("tmp_dir", "data/tmp")
    return str(_repo_root(cfg) / tmp_dir / Path(*parts))
