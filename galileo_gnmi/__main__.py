"""Package entry point."""


def main() -> None:
    """Print a read-only monitoring hint without contacting a device."""
    print("Galileo gNMI API")
    print("Use GalileoTerraFlexAPI with ReadOnlyGNMISession.from_environment().")


if __name__ == "__main__":
    main()
