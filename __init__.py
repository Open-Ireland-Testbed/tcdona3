__version__ = "1.1.0"
# tcdona3/__init__.py
"""
Top-level package for the Open Ireland Optical Testbed v3 control code.

Submodules are imported optionally: if a device module cannot be imported
(e.g. missing vendor libraries, hardware-specific dependencies), it is
silently skipped. Only successfully imported symbols are exposed in
`tcdona3.__all__`.
"""

from importlib import import_module

__all__ = []


def _try_import(module_name: str, names: list, aliases: dict = None) -> None:
    """
    Try to import `tcdona3.<module_name>` and re-export the given
    attribute names if present, and each `aliases` key as another name
    of the attribute it maps to. Failures are ignored so that
    `import tcdona3` always succeeds.
    """
    try:
        mod = import_module(f".{module_name}", __name__)
    except ImportError:
        # Optional dependency or environment-specific module failed to import
        return

    exports = [(name, name) for name in names] + sorted((aliases or {}).items())
    for name, attribute in exports:
        if hasattr(mod, attribute):
            globals()[name] = getattr(mod, attribute)
            if name not in __all__:
                __all__.append(name)


# Core / top-level devices
_try_import("polatis", ["Polatis", "PolatisNetconf"])
_try_import("polatis_old", ["Polatis"])
_try_import("lumentum", ["Lumentum"])
_try_import("ila", ["ILA"])
_try_import("cassini", ["Cassini"])
_try_import("dicon", ["Dicon"])
_try_import("bbsource", ["BBS"])

# Monitoring
_try_import("monitor", ["Monitor", "RoadmMonitor"])

# OSA and related
_try_import("osa", ["OSA"])
# Other instruments
# The class names, and the names this file always meant to export as aliases.
_try_import("yokogawa", ["YokogawaOSA"], {"Yokogawa": "YokogawaOSA"})
_try_import("quadflex", ["QFlex"], {"QuadFlex": "QFlex"})
_try_import("teraflex", ["TFlex"], {"TeraFlex": "TFlex"})
_try_import("teraflex_paramiko", ["TeraflexSSH"], {"TeraFlexParamiko": "TeraflexSSH"})

# Utilities (functions/constants only; safe to expose if import works)
_try_import("utils", ["check_patch_owners", "load_csv_with_pandas"])  # extend if needed

# ApexOSA subpackage
_try_import("ApexOSA.apextls", ["ApexTLS"])
_try_import("ApexOSA.osa", ["ApexOSA"])
_try_import("ApexOSA.filter", ["ApexFilter"])
_try_import("ApexOSA.ocsa", ["OCSA"])
_try_import("ApexOSA.powermeter", ["PowerMeter"])
_try_import("ApexOSA.polarimeter", ["Polarimeter"])
_try_import("ApexOSA.Errors", ["ApexError"])
_try_import("ApexOSA.Constantes", [])  # mostly constants; import only if you want to re-export them by name
_try_import("ApexOSA.Common", [])      # same as above

