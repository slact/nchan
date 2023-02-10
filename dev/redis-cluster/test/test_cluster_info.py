import time
import unittest
import os
import pathlib
from cluster_info import get_cluster_info


class TestClusterInfo(unittest.TestCase):
    def test_info(self):
        file_path = str(pathlib.Path().resolve()) + "/cluster_info.txt"
        self.assertFalse(os.path.exists(file_path))
        get_cluster_info()
        time.sleep(10)
        self.assertTrue(os.path.exists(file_path))
        

if __name__ == '__main__':
    unittest.main()