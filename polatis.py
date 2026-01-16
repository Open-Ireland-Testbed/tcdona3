"""
Shim module for the new NETCONF-based Polatis implementation.
This module exports the 'Polatis' class from the new implementation.
It also exports 'PolatisNetconf' for backward compatibility.
"""

try:
    from .polatis.polatis import Polatis, PolatisNetconf
except ImportError:
    # If the sub-package cannot be imported (e.g. missing dependencies), re-raise.
    raise

__all__ = ["Polatis", "PolatisNetconf"]
