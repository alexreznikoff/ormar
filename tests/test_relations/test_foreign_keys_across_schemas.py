from typing import Optional

import databases
import pytest
import sqlalchemy

import ormar
from tests.settings import DATABASE_URL

database = databases.Database(DATABASE_URL, force_rollback=True)
metadata_case_1 = sqlalchemy.MetaData()
metadata_case_2 = sqlalchemy.MetaData()
metadata_case_3 = sqlalchemy.MetaData()
metadata_case_4 = sqlalchemy.MetaData()

album_schema = "album_schema"
track_schema = "track_schema"


class AlbumCase1(ormar.Model):
    class Meta:
        tablename = "albums"
        metadata = metadata_case_1
        database = database

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class TrackCase1(ormar.Model):
    class Meta:
        tablename = "tracks"
        metadata = metadata_case_1
        database = database

    id: int = ormar.Integer(primary_key=True)
    album: Optional[AlbumCase1] = ormar.ForeignKey(
        AlbumCase1, related_name="track_list"
    )
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()


class AlbumCase2(ormar.Model):
    class Meta:
        tablename = "albums"
        schema = album_schema
        metadata = metadata_case_2
        database = database

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class TrackCase2(ormar.Model):
    class Meta:
        tablename = "tracks"
        schema = track_schema
        metadata = metadata_case_2
        database = database

    id: int = ormar.Integer(primary_key=True)
    album: Optional[AlbumCase2] = ormar.ForeignKey(
        AlbumCase2, related_name="track_list"
    )
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()


class AlbumCase3(ormar.Model):
    class Meta:
        tablename = "albums"
        metadata = metadata_case_3
        database = database

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class TrackCase3(ormar.Model):
    class Meta:
        tablename = "tracks"
        schema = track_schema
        metadata = metadata_case_3
        database = database

    id: int = ormar.Integer(primary_key=True)
    album: Optional[AlbumCase3] = ormar.ForeignKey(
        AlbumCase3, related_name="track_list"
    )
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()


class AlbumCase4(ormar.Model):
    class Meta:
        tablename = "albums"
        schema = album_schema
        metadata = metadata_case_4
        database = database

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class TrackCase4(ormar.Model):
    class Meta:
        tablename = "tracks"
        metadata = metadata_case_4
        database = database

    id: int = ormar.Integer(primary_key=True)
    album: Optional[AlbumCase4] = ormar.ForeignKey(
        AlbumCase4, related_name="track_list"
    )
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()


@pytest.fixture()
def create_test_database(request):
    album_model, track_model = request.param
    engine = sqlalchemy.create_engine(DATABASE_URL)
    engine.execute(sqlalchemy.schema.CreateSchema(album_schema))
    engine.execute(sqlalchemy.schema.CreateSchema(track_schema))

    album_model.Meta.metadata.create_all(engine)

    yield album_model, track_model

    album_model.Meta.metadata.drop_all(engine)
    engine.execute(sqlalchemy.schema.DropSchema(album_schema, cascade=True))
    engine.execute(sqlalchemy.schema.DropSchema(track_schema, cascade=True))


@pytest.mark.asyncio
@pytest.mark.skipif(
    not DATABASE_URL.startswith("postgresql"),
    reason="Requires postgresql database",
)
@pytest.mark.parametrize(
    "create_test_database",
    [
        (AlbumCase1, TrackCase1),
        (AlbumCase2, TrackCase2),
        (AlbumCase3, TrackCase3),
        (AlbumCase4, TrackCase4),
    ],
    indirect=["create_test_database"],
    ids=[
        "no_schemas",
        "two_different_schemas",
        "track_schema_only",
        "album_schema_only",
    ],
)
async def test_model_crud_with_schemas(create_test_database):
    album_model, track_model = create_test_database
    async with database:
        async with database.transaction(force_rollback=True):
            album = album_model(name="Jamaica")
            await album.save()
            track1 = track_model(album=album, title="The Bird", position=1)
            track2 = track_model(
                album=album, title="Heart don't stand a chance", position=2
            )
            track3 = track_model(album=album, title="The Waters", position=3)
            await track1.save()
            await track2.save()
            await track3.save()

            assert len(album.track_list) == 3
            assert album.track_list[1].title == "Heart don't stand a chance"

            track = await track_model.objects.get(title="The Bird")
            assert track.album.pk == album.pk
            assert isinstance(track.album, ormar.Model)
            assert track.album.name is None
            await track.album.load()
            assert track.album.name == "Jamaica"

            track = await track_model.objects.select_related("album").get(
                title="The Bird"
            )
            assert track.album.pk == album.pk
            assert track.album.name == "Jamaica"

            album1 = await album_model.objects.get(name="Jamaica")
            assert album1.pk == album.pk
            assert album1.track_list == []

            album2 = await album_model.objects.prefetch_related("track_list").get(
                name="Jamaica"
            )
            assert album2.pk == album.pk
            assert len(album2.track_list) == 3

            album3 = await album_model.objects.get(name="Jamaica")
            assert album3.pk == album.pk
            assert len(await album3.track_list.all()) == 3
