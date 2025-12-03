"""
Master test runner - runs all test suites
"""
import sys
import os
import importlib

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def run_test_module(module_name):
    """Import and run a test module"""
    print(f"\n{'#'*70}")
    print(f"# Running: {module_name}")
    print(f"{'#'*70}\n")
    
    try:
        # Import the test module
        module = importlib.import_module(f'tests.{module_name}')
        
        # Run the main function
        success = module.main()
        
        return success
    except Exception as e:
        print(f"\n[ERROR] Failed to run {module_name}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all test suites"""
    print("\n" + "="*70)
    print("RUNNING ALL TEST SUITES")
    print("="*70)
    
    # Define test modules to run
    test_modules = [
        'test_select',
        'test_join',
        'test_orderby',
        'test_dml',
        'test_batch_update',
        'test_update_comprehensive',
    ]
    
    results = {}
    
    # Run each test suite
    for module_name in test_modules:
        success = run_test_module(module_name)
        results[module_name] = success
    
    # Overall summary
    print("\n" + "="*70)
    print("OVERALL TEST SUMMARY")
    print("="*70)
    
    for module_name, success in results.items():
        status = "[OK]" if success else "[FAILED]"
        print(f"{status} {module_name}")
    
    total = len(results)
    passed = sum(results.values())
    
    print(f"\nTest suites: {passed}/{total} passed")
    
    if passed == total:
        print("\n[OK] ALL TEST SUITES PASSED!")
        return True
    else:
        print(f"\n[FAILED] {total - passed} test suite(s) failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
