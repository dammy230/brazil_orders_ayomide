import sys
import os
import unittest


sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


"""if __name__ == '__main__':
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    
    sys.exit(not result.wasSuccessful())"""