import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import faker
    import numpy as np
    import polars as pl
    return faker, mo, np, pl


@app.cell
def _(mo):
    citypop = mo.sql(
        f"""
        select population, population / sum(population) OVER () as population_perc, city_name, latitude, longitude from 'data/citypop.csv'
        """
    )
    return (citypop,)


@app.cell
def _(citypop, faker, np, pl):
    import dataclasses
    import datetime
    from enum import StrEnum, auto

    fake = faker.Faker()


    class Sex(StrEnum):
        M = auto()
        F = auto()
        X = auto()


    class TransportMode(StrEnum):
        flight = auto()
        train = auto()


    @dataclasses.dataclass
    class User:
        id: int
        first_name: str
        last_name: str
        sex: Sex
        date_of_birth: datetime.date
        hometown: str
        latitude: float
        longitude: float

        # preferences
        preferred_temperature: int
        preferred_travel_time: int
        preferred_transport_mode: TransportMode


    def generate_users(n: int):
        today = datetime.date.today()
        users = []

        for i in range(n):
            sex = np.random.choice(Sex, p=[0.45, 0.45, 0.1])

            match sex:
                case Sex.M:
                    first_name = fake.first_name_male()
                case Sex.F:
                    first_name = fake.first_name_female()
                case Sex.X:
                    first_name = fake.first_name_nonbinary()

            last_name = fake.last_name()
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80)
            index = np.random.choice(range(len(citypop)), p=citypop["population_perc"])
            row = citypop.row(index, named=True)
            hometown, latitude, longitude = row["city_name"], row["latitude"], row["longitude"]

            temperature = int(np.random.normal(20, 10))
            travel_time = int(abs(np.random.normal(6, 10)))
            if travel_time > 8:
                transport_mode = TransportMode.train
            else:
                transport_mode = np.random.choice(TransportMode)

            users.append(
                User(
                    id=i,
                    first_name=first_name,
                    last_name=last_name,
                    sex=sex,
                    date_of_birth=date_of_birth,
                    hometown=hometown,
                    latitude=latitude,
                    longitude=longitude,
                    preferred_temperature=temperature,
                    preferred_travel_time=travel_time,
                    preferred_transport_mode=transport_mode,
                )
            )

        return pl.DataFrame(users)
    return (generate_users,)


@app.cell
def _(generate_users):
    users = generate_users(1000)
    users.write_csv('data/users.csv')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
