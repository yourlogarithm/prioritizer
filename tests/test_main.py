import unittest
from neo4j import AsyncGraphDatabase
import src.main as main


class TestMain(unittest.IsolatedAsyncioTestCase):
    async def test_get_rank(self):
        driver = AsyncGraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        async with driver.session(database='test-pages') as session:
            await session.run("MATCH (n) DETACH DELETE n")  # Clear the database
            await session.run("CREATE (page:Page {url: 'https://example.com', rank: 10})")  # Add test data
            record = await session.execute_read(main.get_rank, "https://example.com")
            await session.run("MATCH (n) DETACH DELETE n")

        # Assert
        self.assertEqual(record[0], 10)
        self.assertEqual(len(record), 1)
