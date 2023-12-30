from sqlalchemy import (
    create_engine,
    MetaData,
    Column,
    Integer,
    String,
    Table,
    Date,
    Float,
)
import pandas as pd


def main():
    engine = create_engine("sqlite:///database.db", echo=False)

    meta = MetaData()
    # station,date,precip,tobs

    measure = Table(
        "measure",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("date", Date),
        Column("precip", Float),
        Column("tobs", Integer),
    )
    # station,latitude,longitude,elevation,name,country,state

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

    conn = engine.raw_connection()
    cursor = conn.cursor()

    data_measure = pd.read_csv("clean_measure.csv")

    for row in data_measure.itertuples():
        cursor.execute(
            """
                    INSERT INTO measure (station, date, precip, tobs)
                    VALUES (?,?,?,?)
                    """,
            (row.station, row.date, row.precip, row.tobs),
        )

    data_stations = pd.read_csv("clean_stations.csv")

    for row in data_stations.itertuples():
        cursor.execute(
            """
                    INSERT INTO stations (station, latitude, longitude, elevation, name, country, state)
                    VALUES (?,?,?,?,?,?,?)
                    """,
            (
                row.station,
                row.latitude,
                row.longitude,
                row.elevation,
                row.name,
                row.country,
                row.state,
            ),
        )

    conn.commit()

    print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())


if __name__ == "__main__":
    main()
# W sumie to moge uzyć pandas i tosql, wtedy nic nie trzeba robić ani definiować Tables ani nic.
# df.to_sql(measure, con=engine, if_exists='replace', index=False)
