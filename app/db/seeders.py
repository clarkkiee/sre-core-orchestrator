"""Database seeders for initial data."""
import argparse
import asyncio

from uuid import uuid4

from sqlalchemy import select

from app.core.security import hash_password
from app.db.session import async_session_maker
from app.models.user import User

async def create_admin_user(count: int) -> None:
    async with async_session_maker() as session:
        # Check if admin already exists
        result = await session.execute(
            select(User).where(User.is_admin == True)
        )
        existing_admin = result.scalars().all()

        to_create = count - len(existing_admin)

        if to_create <= 0:
            print("Admin user is already exists")
            return

        start_index = len(existing_admin)

        admins = []
        for i in range(start_index, start_index + to_create):
            admin_user = User(
                id=uuid4(),
                email=f"admin{i}@gmail.com",
                hashed_password=hash_password("adminsre"),
                is_admin=True,
                is_active=True,
            )

            admins.append(admin_user)

        session.add_all(admins)
        await session.commit()
        print(f"{len(admins)} admin user(s) created successfully.")

async def seed_database(count: int) -> None:
    """Run all seeders."""
    print("Starting database seeding...")
    await create_admin_user(count)
    print("Database seeding completed.")


def run_seeder() -> None:
    """Entry point for running seeders."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True, help="Number of admins to generate")
    args = parser.parse_args()
    asyncio.run(seed_database(args.count))


if __name__ == "__main__":
    run_seeder()
