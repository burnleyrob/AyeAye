"""
Example of how to write an NDJSON document to an SQLite database.

Before running this install SQLite on the host machine.


Created on 11 Oct 2024

@author: burnleyrob
"""

import ayeaye
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class FlyingAnimal(Base):
    __tablename__ = "flying_animal"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    where = Column(String(50))
    scientific_classification_class = Column(String(50))


class ExampleSqLiteWrite(ayeaye.Model):
    """
    Go through all the animals of flight in the input NDJSON file and output them as they are into
    a table of an SQLite database.
    """

    animals_in = ayeaye.Connect(
        engine_url="ndjson://data/flying_animals.ndjson",
    )

    animals_out = ayeaye.Connect(
        engine_url="sqlite:///aye-aye-dev/Example/animal.db",
        access=ayeaye.AccessMode.WRITE,
        schema_model=FlyingAnimal,
    )

    def build(self):
        self.animals_out.create_table_schema()

        for animal in self.animals_in:
            self.animals_out.add(
                {
                    "name": animal.name,
                    "where": animal.where,
                    "scientific_classification_class": animal.scientific_classification_class,
                }
            )
            self.stats["animals_loaded"] += 1

        self.animals_out.commit()

        self.log("All done!")


if __name__ == "__main__":
    model = ExampleSqLiteWrite()
    model.go()
