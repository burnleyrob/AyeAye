"""
Example of how to write a compressed JSON document into an AWS S3 bucket.

Before running this set up your ~/.aws/credentials file and optionally
`export AWS_PROFILE=profile_name` if your credentials file has multiple credentials.
Also create the bucket and give it suitable permissions!


Created on 12 Sept 2024

@author: si
"""

import ayeaye


class ExampleS3Write(ayeaye.Model):
    """
    Bit of a fake example as all the data and the dataset connection urls are hard coded.
    The data comes from https://www.w3schools.com/colors/colors_hex.asp

    Also see :class:`ExampleS3Read`
    """

    colour_pallet_a = ayeaye.Connect(
        engine_url="gz+s3+csv://aye-aye-dev/ExampleS3/colours.csv.gz",
        access=ayeaye.AccessMode.WRITE,
    )

    colour_pallet_b = ayeaye.Connect(
        engine_url="gz+s3+ndjson://aye-aye-dev/ExampleS3/colours.ndjson.gz",
        access=ayeaye.AccessMode.WRITE,
    )

    colour_pallet_c = ayeaye.Connect(
        engine_url="gz+s3+json://aye-aye-dev/ExampleS3/colours.json.gz",
        access=ayeaye.AccessMode.WRITE,
    )

    def build(self):
        some_colours = [
            ("#000000", "Black"),
            ("#000080", "Navy"),
            ("#00008B", "DarkBlue"),
            ("#0000CD", "MediumBlue"),
            ("#0000FF", "Blue"),
            ("#006400", "DarkGreen"),
            ("#008000", "Green"),
            ("#008080", "Teal"),
            ("#008B8B", "DarkCyan"),
        ]

        # simple mapping output as a JSON doc
        doc = {}
        for hex_value, name in some_colours:

            single_record = {"name": name, "hex_value": hex_value}

            # _a is a CSV file
            self.colour_pallet_a.add(single_record)

            # _b is a new line delimited json file
            self.colour_pallet_b.add(single_record)

            # _c is a single JSON doc. It's written below
            doc[name] = hex_value

        # Updating .data writes the JSON doc.
        self.colour_pallet_c.data = doc
        self.log("All done!")


if __name__ == "__main__":
    model = ExampleS3Write()
    model.go()
