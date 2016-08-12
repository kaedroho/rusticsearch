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


if __name__ == '__main__':
    unittest.main()
