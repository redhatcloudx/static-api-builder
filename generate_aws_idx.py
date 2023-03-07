"""Simple AWS builder."""
from glob import glob
from os import makedirs
from os.path import basename

import pandas as pd


AWS_JSON_DIR = "cloud-json/aws"
AWS_JSON_GLOB = f"{AWS_JSON_DIR}/*.json"
AWS_IDX_DIR = "public/idx/aws"


def get_regions() -> list:
    """Get the regions from the data."""
    return [basename(x).split(".")[0] for x in glob(AWS_JSON_GLOB)]


def read_data():
    """Generate a dataframe containing all of the region data."""
    print("Reading data from JSON files...")
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
    print("Writing images based on OwnerId")
    for owner_id in df.OwnerId.unique():
        owner_df = df[df.OwnerId == owner_id]

        # Exclude any owners with less than 10 images.
        if len(owner_df.index) < 10:
            continue

        print(f"Writing owner {owner_id} to file")
        output_dir = f"{AWS_IDX_DIR}/ownerid/{owner_id}"
        makedirs(output_dir)
        owner_df.to_json(f"{output_dir}/index.json", orient="records")


def write_image_ids(df):
    """Split the data frames by ImageID."""
    print("Writing images based on ImageId")
    for image_id in df.ImageId.unique():
        image_df = df[df.ImageId == image_id]

        output_dir = f"{AWS_IDX_DIR}/imageid/{image_id}"
        makedirs(output_dir)
        image_df.to_json(f"{output_dir}/index.json", orient="records")


def main():
    """Main function."""
    df = read_data()
    df.info()
    print(f"Total rows: {len(df.index)}")
    print(f"Unique ImageID: {len(df.ImageId.unique())}")
    write_owner_ids(df)
    write_image_ids(df)


if __name__ == "__main__":
    main()
