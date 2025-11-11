# -*- coding: utf-8 -*-
"""
General helper utilities for the S3 client CLI.
"""

import functools
import logging
import sys
import time


def setup_logging(log_level, date_format=None):
    """
    Configure logging.

    Arguments:
        log_level   (int): Logging level constant (e.g., logging.DEBUG)
    Keyword arguments (opt):
        date_format (str): Date format in strftime format.
                           Default: %Y-%m-%d %H:%M:%S
    """
    if not date_format:
        date_format = "%Y-%m-%d %H:%M:%S"

    log_fmt = "%(asctime)s %(module)s %(funcName)s %(levelname)s %(message)s"
    formatter = logging.Formatter(fmt=log_fmt, datefmt=date_format)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(log_level)
    root_logger.addHandler(handler)


def msg(color, msg_text, exitcode=0, **print_opts):
    """
    Print colored text.

    Arguments:
        color          (str): color name (blue, red, green, yellow,
                              cyan or nocolor)
        msg_text       (str): text to be printed
        exitcode  (int, opt): Optional parameter. If exitcode is different
                              from zero, it terminates the script, i.e,
                              it calls sys.exit with the exitcode informed

    Optional keyword-only print options:
        end     (str): appended after "msg_text" (default: "\n")
        flush   (bool): whether to forcibly flush the stream (default: True)
        output  (stream): a file-like object (default: sys.stdout)

    Example:
        msg("blue", "nice text in blue")
        msg("red", "Error in my script. terminating", 1)
    """
    # Extract supported print options with defaults
    end = print_opts.pop("end", "\n")
    flush = print_opts.pop("flush", True)
    output = print_opts.pop("output", None) or sys.stdout

    color_dic = {
        "blue": "\033[0;34m",
        "red": "\033[1;31m",
        "green": "\033[0;32m",
        "yellow": "\033[0;33m",
        "cyan": "\033[0;36m",
        "resetcolor": "\033[0m",
    }

    if not color or color == "nocolor":
        print(msg_text, end=end, file=output, flush=flush)
    else:
        if color not in color_dic:
            raise ValueError("Invalid color")
        print(
            f"{color_dic[color]}{msg_text}{color_dic['resetcolor']}",
            end=end,
            file=output,
            flush=flush,
        )

    if exitcode:
        sys.exit(exitcode)


def time_elapsed(func):
    """
    Calculate and print the elapsed time in seconds of a function execution.

    This decorator measures how long the wrapped function takes to run.
    After execution, it prints the elapsed time in seconds.

    Arguments:
        func (function): Function to be wrapped by the decorator.

    Returns:
        function: Wrapped function that executes the original callable and
                  prints the elapsed time upon completion.
    """

    @functools.wraps(func)
    def wrapped_f(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        # keep track of total elapsed time for all execution of the function
        wrapped_f.elapsed += elapsed_time

        output = f"  - Elapsed time {elapsed_time:.4f} seconds"
        msg("nocolor", output)

        return result

    wrapped_f.elapsed = 0
    return wrapped_f


def bytes2human(size, *, unit="", precision=2, base=1024):
    """
    Convert number in bytes to human format.

    Arguments:
        size       (int): bytes to be converted

    Keyword arguments (opt):
        unit       (str):  The unit to convert to. Must be one of
                           ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
        precision  (int): number of digits after the decimal point
        base       (int): Conversion base.
                          Use 1000 - for decimal base
                          Use 1024 - for binary base (default)

    Returns:
        tuple[str, str]: A tuple containing:
            - The converted value as a formatted string (e.g., "1.25")
            - The corresponding unit (e.g., "MB")
    """
    # validate parameters
    if not isinstance(precision, int):
        raise ValueError("precision is not a number")
    if not isinstance(base, int):
        raise ValueError("base is not a number")
    try:
        num = float(size)
    except ValueError as exc:
        raise ValueError("value is not a number") from exc

    suffix = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"]

    # If it needs to convert bytes to a specific unit
    if unit:
        try:
            num = num / base ** suffix.index(unit)
        except ValueError as exc:
            raise ValueError(f"Error: unit must be {', '.join(suffix[1:])}") from exc
        return f"{num:.{precision}f}", unit

    # Calculate the greatest unit for the that size
    for counter, suffix_unit in enumerate(suffix):
        if num < base:
            return f"{num:.{precision}f}", suffix_unit
        if counter == len(suffix) - 1:
            raise ValueError("value greater than the highest unit")
        num /= base

    return f"{num:.{precision}f}", suffix_unit
