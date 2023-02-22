"""Simple AWS builder."""
from os import makedirs

import pandas as pd

from apibuilder import config


def read_data() -> pd.DataFrame:
    """Read the raw AWS image data."""
    return pd.read_json(config.AWS_RAW_DATA, compression="zstd")


def regions(df: pd.DataFrame) -> list:
    """Return a list of AWS regions."""
    return sorted(df["Region"].unique().tolist())


def write_region(df: pd.DataFrame, region: str) -> None:
    """Write data for a single AWS region."""
    destination = f"public/{region}"
    makedirs(destination, exist_ok=True)
    region_df = df[df.Region == region]

    for i in df.index:
        row = df.loc[i]
        row.to_json(f"{destination}/{row.ImageId}.json")


def write_all_regions(df: pd.DataFrame) -> None:
    """Write data for all AWS regions."""
    for region in regions(df):
        write_region(df, region)


def main() -> None:
    """Main function."""
    df = read_data()
    write_all_regions(df)
