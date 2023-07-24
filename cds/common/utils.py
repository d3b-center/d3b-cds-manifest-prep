from pathlib import Path

from d3b_cavatica_tools.utils.logging import get_logger


def mkdir_if_not_exists(dir_path):
    logger = get_logger(__name__, testing_mode=False)
    if not isinstance(dir_path, Path):
        raise TypeError(f"{dir_path} is not {Path}")
    elif not dir_path.exists():
        logger.debug(f"creating cache directory: {dir_path}")
        dir_path.mkdir(parents=True)
    elif dir_path.is_file():
        raise ValueError(f"{dir_path} is a file. Function expects a directory")
    else:
        logger.debug(f"{dir_path} already exists, not creating directory")
