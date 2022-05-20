import redis
import unittest

from redis_utils import *

class RedisTests(unittest.TestCase):    

    @classmethod
    def setUpClass(cls):
        cls._r = connect_redis()

    def testing_key_value_pair(self):
        r= self._r        
        value = "bar"
        self.assertTrue( r.set("foo",value) )
        self.assertEqual(value, r.get("foo") )

    def testing_multiple_keys(self):
        r= self._r
        key1 = "Jupiter"
        value1 = "planet"
        key2 = "Sun"
        value2 ="star"
        self.assertTrue( r.mset({key1: value1, key2: value2}) )
        self.assertEqual( value1, r.get(key1) )
        self.assertEqual( value2, r.get(key2) )

    def testing_hash(self):
        r = self._r
        hash_name  = "captain"
        key1   = "enterprise"
        value1 = "kirk"
        key2   = "voyager"
        value2 = "janeway"
        r.hset(hash_name, key1, value1)
        r.hset(hash_name, key2, value2)
        self.assertEqual(value1, r.hget(hash_name, key1))
        self.assertEqual(value2, r.hget(hash_name, key2))

    def testing_lists(self):
        r = self._r
        set_name = "ships"
        value1   = "enterprise"
        value2   = "deep space nine"
        r.lpush(set_name, value1)
        r.lpush(set_name, value2)
        self.assertEqual(value1, r.rpop(set_name))
        self.assertEqual(value2, r.rpop(set_name))

if __name__ == "__main__":
    unittest.main()