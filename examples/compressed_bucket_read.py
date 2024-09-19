"""
Example of how to read all the compressed JSON documents from an AWS S3 bucket that match a pattern.

See :class:`ExampleS3Write` for how 'this' file was built.

Before running this set up your ~/.aws/credentials file and optionally
`export AWS_PROFILE=profile_name` if your credentials file has multiple credentials.
Also create the bucket and file and ensure there are the correct permissions!


Created on 12 Sept 2024

@author: si
"""

import ayeaye


class ExampleS3Read(ayeaye.Model):

    colour_pallets = ayeaye.Connect(engine_url="gz+s3+json://aye-aye-dev/ExampleS3/*.json.gz")

    def build(self):

        for pallet_dataset in self.colour_pallets:

            self.log(f"Reading from dataset: {pallet_dataset.engine_url}")

            for name, hex_value in pallet_dataset.data.items():
                self.log(f"{name} - {hex_value}")


if __name__ == "__main__":
    model = ExampleS3Read()
    model.go()
