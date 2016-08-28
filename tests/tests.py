import sys
import unittest

from elasticsearch import Elasticsearch


class RusticsearchTestCase(unittest.TestCase):
    def setUp(self):
        self.es = Elasticsearch()


class TestIndex(RusticsearchTestCase):
    def test_put_index_exists_and_delete(self):
        self.es.indices.create(index='foo')
        self.assertTrue(self.es.indices.exists(index='foo'))

        self.es.indices.delete(index='foo')
        self.assertFalse(self.es.indices.exists(index='foo'))

    def test_exists_on_non_existant_index(self):
        self.assertFalse(self.es.indices.exists(index='foo'))


class TestAlias(RusticsearchTestCase):
    def test_put_alias_exists_and_delete(self):
        # Create index
        self.es.indices.create(index='foo')

        # Create alias
        self.es.indices.put_alias(index='foo', name='alias')

        # Check the alias exists
        self.assertTrue(self.es.indices.exists(index='alias'))
        self.assertTrue(self.es.indices.exists_alias(name='alias'))
        self.assertTrue(self.es.indices.exists_alias(index='foo', name='alias'))
        self.assertFalse(self.es.indices.exists_alias(index='bar', name='alias'))

        # Now delete the index by the alias
        self.es.indices.delete(index='alias')
        self.assertFalse(self.es.indices.exists(index='alias'))
        self.assertFalse(self.es.indices.exists_alias(name='alias'))
        self.assertFalse(self.es.indices.exists(index='foo'))


if __name__ == '__main__':
    unittest.main()
