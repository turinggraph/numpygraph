# -*- coding:utf-8 -*-
# @pytest.fixture(scope='function')
# def setup_function(request):
#     def teardown_function():
#         print("teardown_function called.")
#     request.addfinalizer(teardown_function)  # 此内嵌函数做teardown工作
#     print('setup_function called.')

# @pytest.fixture(scope='module') 
# def setup_module(request):
#     def teardown_module():
#         print("teardown_module called.")
#     request.addfinalizer(teardown_module)
#     print('setup_module called.')

# @pytest.mark.website
# def test_1(setup_function):
#     print('Test_1 called.')


def test_2():
    print('Test_2 called.')


def test_4():
    print('Test_4 called.')
    assert 2 == 1 + 1
