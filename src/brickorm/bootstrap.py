from sqlmodel import Session, SQLModel, create_engine
from brickorm.models import ALL_MODELS
import tqdm


def bootstrap(path: str):
    engine = create_engine(f"sqlite:///{path}")

    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        for model in tqdm.tqdm(ALL_MODELS):
            for instance in model.download_instances():
                session.add(instance)
            session.commit()


if __name__ == "__main__":
    bootstrap("database.db")
