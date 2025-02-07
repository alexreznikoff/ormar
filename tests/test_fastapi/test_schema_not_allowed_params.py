import databases
import sqlalchemy

import ormar

DATABASE_URL = "sqlite:///db.sqlite"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()


class BaseMeta(ormar.ModelMeta):
    metadata = metadata
    database = database


class Author(ormar.Model):
    class Meta(BaseMeta):
        tablename = "authors"

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    contents: str = ormar.Text()


def test_schema_not_allowed():
    schema = Author.schema()
    for field_schema in schema.get("properties").values():
        for key in field_schema.keys():
            assert "_" not in key, f"Found illegal field in openapi schema: {key}"
