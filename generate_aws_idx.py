"""Simple AWS builder."""
import logging
from glob import glob
from os import makedirs
from os.path import basename

import pandas as pd

from apibuilder import config


AWS_JSON_DIR = "cloud-json/aws"
AWS_JSON_GLOB = f"{AWS_JSON_DIR}/*.json"
AWS_IDX_DIR = "public/idx/aws/ownerid"


logger = logging.getLogger("aws")
logger.setLevel(logging.DEBUG)


def get_regions() -> list:
    """Get the regions from the data."""
    return [basename(x).split(".")[0] for x in glob(AWS_JSON_GLOB)]


def read_data():
    """Generate a dataframe containing all of the region data."""
    logger.info("Reading data from JSON files...")
    data_frames = [read_json(region) for region in get_regions()]
    df = pd.concat(data_frames, ignore_index=True)
    df.sort_values(by=["CreationDate"], inplace=True)
    return df


def read_json(region: str) -> pd.DataFrame:
    """Read the json data for a region."""
    filename = f"{AWS_JSON_DIR}/{region}.json"
    temp_df = pd.read_json(filename, orient="records")

    # Create a view of the data.
    schema_fields = [
        "ImageId",
        "OwnerId",
        "Name",
        "Architecture",
        "VirtualizationType",
        "CreationDate",
    ]
    region_view = temp_df[schema_fields].copy()

    # Add the region to the view.
    region_view["Region"] = region
    return region_view


def write_owner_ids(df):
    """Split the data frames by OwnerId."""
    for owner_id in df.OwnerId.unique():
        owner_df = df[df.OwnerId == owner_id]

        # Exclude any owners with less than 10 images.
        if len(owner_df.index) < 10:
            continue

        output_file = f"{AWS_IDX_DIR}/{owner_id}/index.html"
        logger.info(f"Writing {owner_id} to {output_file}")
        owner_df.to_json(output_file, orient="records")


def main():
    """Main function."""
    df = read_data()
    df.info()
    write_owner_ids(df)
