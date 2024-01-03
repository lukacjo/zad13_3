from sqlalchemy import (
    create_engine,
    MetaData,
    Column,
    Integer,
    String,
    Table,
    Date,
    Float,
    delete,
    select,
)
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd


def select_data(session, stmt):
    result = session.execute(stmt)
    rows = result.fetchall()

    for row in rows:
        print(row)


def main():
    engine = create_engine("sqlite:///database.db", echo=False)

    meta = MetaData()

    measure = Table(
        "measure",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("date", Date),
        Column("precip", Float),
        Column("tobs", Integer),
    )

    stations = Table(
        "stations",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )

    meta.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    data_measure = pd.read_csv("clean_measure.csv")
    for row in data_measure.itertuples():
        session.execute(
            measure.insert(),
            {
                "station": row.station,
                "date": datetime.strptime(row.date, "%Y-%m-%d").date(),
                "precip": row.precip,
                "tobs": row.tobs,
            },
        )

    data_stations = pd.read_csv("clean_stations.csv")
    for row in data_stations.itertuples():
        session.execute(
            stations.insert(),
            {
                "station": row.station,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "elevation": row.elevation,
                "name": row.name,
                "country": row.country,
                "state": row.state,
            },
        )

    session.commit()

    stmt = select(stations).limit(5)
    select_data(session, stmt)

    print()

    stmt = select(stations).where(stations.c.elevation == 3.0)
    select_data(session, stmt)

    stmt = delete(stations).where(stations.c.elevation == 3.0)
    session.execute(stmt)

    stmt = delete(stations)
    session.execute(stmt)

    session.commit()


if __name__ == "__main__":
    main()

# W sumie to moge uzyć pandas i tosql, wtedy nic nie trzeba robić ani definiować Tables ani nic.
# df.to_sql(measure, con=engine, if_exists='replace', index=False)
