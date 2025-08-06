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
def _(citypop, faker, np):
    import dataclasses
    import datetime
    from enum import StrEnum, auto

    fake = faker.Faker()


    class Sex(StrEnum):
        M = auto()
        F = auto()
        X = auto()


    @dataclasses.dataclass
    class User:
        first_name: str
        last_name: str
        sex: Sex
        date_of_birth: datetime.date
        hometown: str


    def generate_users(n: int):
        today = datetime.date.today()
        users = []

        for _ in range(n):
            sex = np.random.choice(Sex, p=[0.45, 0.45, 0.1])

            match sex:
                case Sex.M:
                    first_name = fake.first_name_male()
                case Sex.F:
                    first_name = fake.first_name_female()
                case Sex.X:
                    first_name = fake.first_name_nonbinary()

            last_name = fake.last_name()
            date_of_birth = today - datetime.timedelta(days=np.random.randint(18 * 365, 80 * 365))
            hometown = np.random.choice(citypop["city_name"], p=citypop["population_perc"])

            users.append(
                User(first_name=first_name, last_name=last_name, sex=sex, date_of_birth=date_of_birth, hometown=hometown)
            )

        return users
    return (generate_users,)


@app.cell
def _(generate_users):
    users = generate_users(10000)
    return (users,)


@app.cell
def _(pl, users):
    users_df = pl.DataFrame(users)
    users_df.write_csv("data/users.csv")
    return


if __name__ == "__main__":
    app.run()
